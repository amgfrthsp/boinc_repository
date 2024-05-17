use dslab_core::context::SimulationContext;
use dslab_core::{cast, log_error, Event, EventHandler, EventId};
use dslab_core::{component::Id, log_debug};
use dslab_network::{DataTransferCompleted, Network};
use dslab_storage::disk::Disk;
use dslab_storage::events::{
    DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed,
};
use dslab_storage::storage::Storage;
use futures::{select, FutureExt};
use serde::Serialize;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

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
    server_id: Id,
    client_networks: HashMap<Id, Rc<RefCell<Network>>>,
    disk: Rc<RefCell<Disk>>,
    input_files: RefCell<HashMap<WorkunitId, InputFileMetadata>>, // workunit_id -> input files
    output_files: RefCell<HashMap<ResultId, OutputFileMetadata>>, // result_id -> output files
    is_active: bool,
    pub ctx: SimulationContext,
}

impl DataServer {
    pub fn new(disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
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
            server_id: 0,
            client_networks: HashMap::new(),
            disk,
            input_files: RefCell::new(HashMap::new()),
            output_files: RefCell::new(HashMap::new()),
            is_active: true,
            ctx,
        }
    }

    pub fn add_client_network(
        &mut self,
        client_id: Id,
        net: Rc<RefCell<Network>>,
        node_name: &str,
    ) {
        net.borrow_mut().set_location(self.ctx.id(), node_name);
        self.client_networks.insert(client_id, net);
    }

    pub fn set_server_id(&mut self, server_id: Id) {
        self.server_id = server_id;
    }

    pub fn download_input_files_from_server(
        &self,
        input_files: Vec<InputFileMetadata>,
        ref_id: u64,
    ) {
        self.ctx
            .spawn(self.process_download_input_files_from_server(input_files, ref_id));
    }

    async fn process_download_input_files_from_server(
        &self,
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
            self.server_id,
        );
    }

    pub fn on_input_files_inquiry(&self, workunit_id: WorkunitId, ref_id: EventId, client_id: Id) {
        let input_files_ref = self.input_files.borrow();
        let input_file = input_files_ref.get(&workunit_id).unwrap();
        self.upload_input_file_on_client(input_file.size as f64, client_id, ref_id);
    }

    pub fn upload_input_file_on_client(&self, size: f64, to: Id, ref_id: EventId) {
        self.ctx
            .spawn(self.process_upload_input_file(size, to, ref_id));
    }

    async fn process_upload_input_file(&self, size: f64, to: Id, ref_id: EventId) {
        futures::join!(self.process_network_upload(size, to),);
        self.ctx.emit_now(InputFileUploadCompleted { ref_id }, to);
    }

    pub fn download_output_file_from_client(&self, file: DataServerFile, from: Id) {
        self.ctx
            .spawn(self.process_download_output_file(file, from));
    }

    async fn process_download_output_file(&self, file: DataServerFile, from: Id) {
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
        let net = self.client_networks.get(&from).unwrap();
        let net_id = net.borrow().id();
        let transfer_id = net
            .borrow_mut()
            .transfer_data(from, self.server_id, size, self.ctx.id());

        self.ctx
            .recv_event_by_key_from::<DataTransferCompleted>(net_id, transfer_id as u64)
            .await;
    }

    async fn process_network_upload(&self, size: f64, to: Id) {
        let net = self.client_networks.get(&to).unwrap();
        let net_id = net.borrow().id();
        let transfer_id = net
            .borrow_mut()
            .transfer_data(self.server_id, to, size, self.ctx.id());

        self.ctx
            .recv_event_by_key_from::<DataTransferCompleted>(net_id, transfer_id as u64)
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

    async fn process_disk_read(&self, size: u64) {
        let disk_read_id = self.disk.borrow_mut().read(size, self.ctx.id());

        select! {
            _ = self.ctx.recv_event_by_key::<DataReadCompleted>(disk_read_id).fuse() => {
                // log_debug!(self.ctx, "read completed!!!");
            }
            _ = self.ctx.recv_event_by_key::<DataReadFailed>(disk_read_id).fuse() => {
                log_debug!(self.ctx, "read FAILED!!!");
            }
        };
    }

    pub fn delete_input_files(&mut self, workunit_id: WorkunitId) -> u32 {
        let input_file = self.input_files.borrow_mut().remove(&workunit_id);
        if input_file.is_none() {
            log_error!(self.ctx, "No such output file {}", workunit_id);
            return 0;
        }
        log_debug!(self.ctx, "deleted input files for workunit {}", workunit_id);

        return 0;
    }

    pub fn delete_output_files(&mut self, result_id: ResultId) -> u32 {
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

impl EventHandler for DataServer {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            InputFilesInquiry {
                workunit_id,
                ref_id,
            } => {
                if self.is_active {
                    self.on_input_files_inquiry(workunit_id, ref_id, event.src);
                }
            }
            OutputFileFromClient { output_file } => {
                if self.is_active {
                    self.download_output_file_from_client(
                        DataServerFile::Output(output_file),
                        event.src,
                    );
                }
            }
            Finish {} => {
                self.is_active = false;
            }
        })
    }
}
