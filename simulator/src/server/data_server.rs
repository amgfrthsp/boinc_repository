use dslab_core::context::SimulationContext;
use dslab_core::{cast, Event, EventHandler, EventId};
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
    server_id: Id,
    net: Rc<RefCell<Network>>,
    disk: Rc<RefCell<Disk>>,
    input_files: RefCell<HashMap<WorkunitId, InputFileMetadata>>, // workunit_id -> input files
    output_files: RefCell<HashMap<ResultId, OutputFileMetadata>>, // result_id -> output files
    ctx: SimulationContext,
}

impl DataServer {
    pub fn new(net: Rc<RefCell<Network>>, disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
        ctx.register_key_getter_for::<DataTransferCompleted>(|e| e.dt.id as u64);
        ctx.register_key_getter_for::<DataWriteCompleted>(|e| e.request_id);
        ctx.register_key_getter_for::<DataWriteFailed>(|e| e.request_id);
        ctx.register_key_getter_for::<DataReadCompleted>(|e| e.request_id);
        ctx.register_key_getter_for::<DataReadFailed>(|e| e.request_id);
        ctx.register_key_getter_for::<InputFileDownloadCompleted>(|e| e.workunit_id);
        ctx.register_key_getter_for::<OutputFileDownloadCompleted>(|e| e.result_id);
        ctx.register_key_getter_for::<InputFileUploadCompleted>(|e| e.ref_id);

        Self {
            server_id: 0,
            net,
            disk,
            input_files: RefCell::new(HashMap::new()),
            output_files: RefCell::new(HashMap::new()),
            ctx,
        }
    }

    pub fn set_server_id(&mut self, server_id: Id) {
        self.server_id = server_id;
    }

    pub fn on_input_files_inquiry(&self, workunit_id: WorkunitId, ref_id: EventId, client_id: Id) {
        let input_files_ref = self.input_files.borrow();
        let input_file = input_files_ref.get(&workunit_id).unwrap();
        self.upload_file(DataServerFile::Input(input_file.clone()), client_id, ref_id);
    }

    pub fn download_file(&self, file: DataServerFile, from: Id) {
        self.ctx.spawn(self.process_download_file(file, from));
    }

    async fn process_download_file(&self, file: DataServerFile, from: Id) {
        log_debug!(self.ctx, "file download started {:?}", file);

        futures::join!(
            self.process_network_download(file.clone(), from),
            self.process_disk_write(file.clone())
        );

        log_debug!(self.ctx, "file download finished {:?}", file);

        // if retry.contains(file_id) {retry in x} else:

        match file {
            DataServerFile::Input(input_file) => {
                let workunit_id = input_file.workunit_id;

                self.input_files
                    .borrow_mut()
                    .insert(workunit_id, input_file);

                self.ctx
                    .emit_now(InputFileDownloadCompleted { workunit_id }, self.server_id);

                log_debug!(
                    self.ctx,
                    "received a new input file for workunit {}",
                    workunit_id,
                );
            }
            DataServerFile::Output(output_file) => {
                let result_id = output_file.result_id;

                self.output_files
                    .borrow_mut()
                    .insert(result_id, output_file);

                self.ctx
                    .emit_now(OutputFileDownloadCompleted { result_id }, from);

                log_debug!(
                    self.ctx,
                    "received a new output file for result {}",
                    result_id,
                );
            }
        }
    }

    pub fn upload_file(&self, file: DataServerFile, to: Id, ref_id: EventId) {
        self.ctx.spawn(self.process_upload_file(file, to, ref_id));
    }

    async fn process_upload_file(&self, file: DataServerFile, to: Id, ref_id: EventId) {
        if !self.input_files.borrow().contains_key(&file.id())
            && !self.output_files.borrow().contains_key(&file.id())
        {
            // error: no such file
        }

        log_debug!(self.ctx, "file upload started {:?}", file);

        futures::join!(
            self.process_network_upload(file.clone(), to),
            self.process_disk_read(file.clone())
        );

        log_debug!(self.ctx, "file upload finished {:?}", file);

        // if retry.contains(file_id) {retry in x} else:

        match file {
            DataServerFile::Input(input_file) => {
                self.ctx.emit_now(InputFileUploadCompleted { ref_id }, to);

                log_debug!(
                    self.ctx,
                    "uploaded input file for workunit {} to client {}",
                    input_file.workunit_id,
                    to,
                );
            }
            DataServerFile::Output(_output_file) => {}
        }
    }

    async fn process_network_download(&self, file: DataServerFile, from: Id) {
        let transfer_id = self.net.borrow_mut().transfer_data(
            from,
            self.ctx.id(),
            file.size() as f64,
            self.ctx.id(),
        );

        self.ctx
            .recv_event_by_key::<DataTransferCompleted>(transfer_id as u64)
            .await;
    }

    async fn process_network_upload(&self, file: DataServerFile, to: Id) {
        let transfer_id = self.net.borrow_mut().transfer_data(
            self.ctx.id(),
            to,
            file.size() as f64,
            self.ctx.id(),
        );

        self.ctx
            .recv_event_by_key::<DataTransferCompleted>(transfer_id as u64)
            .await;
    }

    async fn process_disk_write(&self, file: DataServerFile) {
        let disk_write_id = self.disk.borrow_mut().write(file.size(), self.ctx.id());

        select! {
            _ = self.ctx.recv_event_by_key::<DataWriteCompleted>(disk_write_id).fuse() => {
                log_debug!(self.ctx, "write completed!!!");
            }
            _ = self.ctx.recv_event_by_key::<DataWriteFailed>(disk_write_id).fuse() => {
                log_debug!(self.ctx, "write FAILED!!!");
            }
        };
    }

    async fn process_disk_read(&self, file: DataServerFile) {
        let disk_read_id = self.disk.borrow_mut().read(file.size(), self.ctx.id());

        select! {
            _ = self.ctx.recv_event_by_key::<DataReadCompleted>(disk_read_id).fuse() => {
                log_debug!(self.ctx, "read completed!!!");
            }
            _ = self.ctx.recv_event_by_key::<DataReadFailed>(disk_read_id).fuse() => {
                log_debug!(self.ctx, "read FAILED!!!");
            }
        };
    }

    pub fn delete_input_files(&mut self, workunit_id: WorkunitId) -> u32 {
        self.input_files.borrow_mut().remove(&workunit_id);
        // add disk free

        log_debug!(self.ctx, "deleted input files for workunit {}", workunit_id,);

        return 0;
    }

    pub fn delete_output_files(&mut self, result_id: ResultId) -> u32 {
        self.output_files.borrow_mut().remove(&result_id);
        // add disk free

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
                self.on_input_files_inquiry(workunit_id, ref_id, event.src);
            }
            OutputFileFromClient { output_file } => {
                self.download_file(DataServerFile::Output(output_file), event.src);
            }
        })
    }
}
