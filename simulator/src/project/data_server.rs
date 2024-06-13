use dslab_core::context::SimulationContext;
use dslab_core::{cast, log_error, Event, EventId, StaticEventHandler};
use dslab_core::{component::Id, log_debug};
use dslab_network::{DataTransferCompleted, Network};
use dslab_storage::disk::Disk;
use dslab_storage::events::{
    DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed,
};
use dslab_storage::storage::Storage;
use futures::{select, FutureExt};
use rustc_hash::FxHashMap;
use serde::Serialize;
use std::{cell::RefCell, rc::Rc};

use crate::common::Finish;

use super::job::{DataServerFile, InputFileMetadata, OutputFileMetadata, ResultId, WorkunitId};

#[derive(Clone, Serialize)]
pub struct InputFilesInquiry {
    pub workunit_id: WorkunitId,
    pub ref_id: EventId,
}

#[derive(Clone, Serialize)]
pub struct OutputFileFromClient {
    pub output_file: OutputFileMetadata,
}

#[derive(Clone, Serialize)]
pub struct InputFileDownloadCompleted {
    pub workunit_id: WorkunitId,
}

#[derive(Clone, Serialize)]
pub struct OutputFileDownloadCompleted {
    pub result_id: ResultId,
}

#[derive(Clone, Serialize)]
pub struct InputFileUploadCompleted {
    pub ref_id: EventId,
}

pub struct DataServer {
    pub id: Id,
    server_id: RefCell<Id>,
    network: Rc<RefCell<Network>>,
    disk: Rc<RefCell<Disk>>,
    input_files: RefCell<FxHashMap<WorkunitId, InputFileMetadata>>, // workunit_id -> input files
    output_files: RefCell<FxHashMap<ResultId, OutputFileMetadata>>, // result_id -> output files
    is_active: RefCell<bool>,
    pub ctx: SimulationContext,
}

impl DataServer {
    pub fn new(
        network: Rc<RefCell<Network>>,
        disk: Rc<RefCell<Disk>>,
        ctx: SimulationContext,
    ) -> Self {
        ctx.register_key_getter_for::<DataTransferCompleted>(|e| e.dt.id as u64);
        ctx.register_key_getter_for::<DataWriteCompleted>(|e| e.request_id);
        ctx.register_key_getter_for::<DataWriteFailed>(|e| e.request_id);
        ctx.register_key_getter_for::<DataReadCompleted>(|e| e.request_id);
        ctx.register_key_getter_for::<DataReadFailed>(|e| e.request_id);
        ctx.register_key_getter_for::<InputFileDownloadCompleted>(|e| e.workunit_id);
        ctx.register_key_getter_for::<OutputFileDownloadCompleted>(|e| e.result_id);
        ctx.register_key_getter_for::<InputFileUploadCompleted>(|e| e.ref_id);

        Self {
            id: ctx.id(),
            server_id: RefCell::new(0),
            network,
            disk,
            input_files: RefCell::new(FxHashMap::default()),
            output_files: RefCell::new(FxHashMap::default()),
            is_active: RefCell::new(true),
            ctx,
        }
    }

    fn is_active(&self) -> bool {
        *self.is_active.borrow()
    }

    fn get_server_id(&self) -> Id {
        *self.server_id.borrow()
    }

    pub fn set_server_id(&self, server_id: Id) {
        *self.server_id.borrow_mut() = server_id;
    }

    pub fn download_input_files_from_server(
        self: Rc<Self>,
        input_files: Vec<InputFileMetadata>,
        ref_id: u64,
    ) {
        self.ctx.spawn(
            self.clone()
                .process_download_input_files_from_server(input_files, ref_id),
        );
    }

    pub async fn process_download_input_files_from_server(
        self: Rc<Self>,
        input_files: Vec<InputFileMetadata>,
        ref_id: u64,
    ) {
        for input_file in input_files {
            self.input_files
                .borrow_mut()
                .insert(input_file.workunit_id, input_file);
        }
        self.ctx.emit_now(
            InputFileDownloadCompleted {
                workunit_id: ref_id,
            },
            self.get_server_id(),
        );
    }

    pub fn on_input_files_inquiry(
        self: Rc<Self>,
        workunit_id: WorkunitId,
        ref_id: EventId,
        client_id: Id,
    ) {
        let input_files_ref = self.input_files.borrow();
        let input_file_size = input_files_ref.get(&workunit_id).unwrap().size;
        drop(input_files_ref);
        self.upload_input_file_on_client(input_file_size as f64, client_id, ref_id);
    }

    pub fn upload_input_file_on_client(self: Rc<Self>, size: f64, to: Id, ref_id: EventId) {
        self.ctx
            .spawn(self.clone().process_upload_input_file(size, to, ref_id));
    }

    async fn process_upload_input_file(self: Rc<Self>, size: f64, to: Id, ref_id: EventId) {
        futures::join!(self.process_network_upload(size, to),);
        self.ctx.emit_now(InputFileUploadCompleted { ref_id }, to);
    }

    pub fn download_output_file_from_client(self: Rc<Self>, file: DataServerFile, from: Id) {
        self.ctx
            .spawn(self.clone().process_download_output_file(file, from));
    }

    async fn process_download_output_file(self: Rc<Self>, file: DataServerFile, from: Id) {
        match file {
            DataServerFile::Input(..) => {}
            DataServerFile::Output(output_file) => {
                futures::join!(
                    self.process_network_download(output_file.size as f64, from),
                    self.process_disk_write(output_file.size)
                );

                let result_id = output_file.result_id;
                self.output_files
                    .borrow_mut()
                    .insert(result_id, output_file);

                self.ctx
                    .emit_now(OutputFileDownloadCompleted { result_id }, from);
            }
        }
    }

    async fn process_network_download(&self, size: f64, from: Id) {
        let transfer_id = self.network.borrow_mut().transfer_data(
            from,
            self.get_server_id(),
            size,
            self.ctx.id(),
        );

        self.ctx
            .recv_event_by_key::<DataTransferCompleted>(transfer_id as u64)
            .await;
    }

    async fn process_network_upload(&self, size: f64, to: Id) {
        let transfer_id =
            self.network
                .borrow_mut()
                .transfer_data(self.get_server_id(), to, size, self.ctx.id());

        self.ctx
            .recv_event_by_key::<DataTransferCompleted>(transfer_id as u64)
            .await;
    }

    async fn process_disk_write(&self, size: u64) {
        let disk_write_id = self.disk.borrow_mut().write(size, self.ctx.id());

        select! {
            _ = self.ctx.recv_event_by_key::<DataWriteCompleted>(disk_write_id).fuse() => {
                // log_debug!(self.ctx, "write completed!!!");
            }
            _ = self.ctx.recv_event_by_key::<DataWriteFailed>(disk_write_id).fuse() => {
                log_debug!(self.ctx, "write FAILED!!!");
            }
        };
    }

    pub fn read_from_disk(&self, size: u64, requester: Id) -> u64 {
        self.disk.borrow_mut().read(size, requester)
    }

    pub fn delete_input_files(&self, workunit_id: WorkunitId) -> u32 {
        let input_file = self.input_files.borrow_mut().remove(&workunit_id);
        if input_file.is_none() {
            log_error!(self.ctx, "No such output file {}", workunit_id);
            return 0;
        }
        log_debug!(self.ctx, "deleted input files for workunit {}", workunit_id);

        return 0;
    }

    pub fn delete_output_files(&self, result_id: ResultId) -> u32 {
        let output_file = self.output_files.borrow_mut().remove(&result_id);
        if output_file.is_none() {
            log_error!(self.ctx, "No such output file {}", result_id);
            return 0;
        }

        self.disk
            .borrow_mut()
            .mark_free(output_file.unwrap().size)
            .expect("Failed to free disk space");

        log_debug!(self.ctx, "deleted output files for result {}", result_id,);

        return 0;
    }
}

impl StaticEventHandler for DataServer {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
            InputFilesInquiry {
                workunit_id,
                ref_id,
            } => {
                if self.is_active() {
                    self.on_input_files_inquiry(workunit_id, ref_id, event.src);
                }
            }
            OutputFileFromClient { output_file } => {
                if self.is_active() {
                    self.download_output_file_from_client(
                        DataServerFile::Output(output_file),
                        event.src,
                    );
                }
            }
            Finish {} => {
                *self.is_active.borrow_mut() = false;
            }
        })
    }
}
