use dslab_core::context::SimulationContext;
use dslab_core::{cast, Event, EventHandler};
use dslab_core::{component::Id, log_debug};
use dslab_network::{DataTransferCompleted, Network};
use dslab_storage::disk::Disk;
use dslab_storage::events::{DataWriteCompleted, DataWriteFailed};
use dslab_storage::storage::Storage;
use futures::{select, FutureExt};
use serde::Serialize;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use super::job::{FileId, InputFileMetadata, OutputFileMetadata, ResultId, WorkunitId};

#[derive(Clone, Serialize)]
pub struct OutputFileFromClient {
    pub output_file: OutputFileMetadata,
}

#[derive(Clone, Serialize)]
pub struct DownloadInputFileCompleted {
    pub workunit_id: WorkunitId,
}

pub struct DataServer {
    server_id: Id,
    net: Rc<RefCell<Network>>,
    disk: Rc<RefCell<Disk>>,
    input_files: RefCell<HashMap<FileId, InputFileMetadata>>, // workunit_id -> input files
    output_files: HashMap<FileId, OutputFileMetadata>,        // result_id -> output files
    ctx: SimulationContext,
}

impl DataServer {
    pub fn new(net: Rc<RefCell<Network>>, disk: Rc<RefCell<Disk>>, ctx: SimulationContext) -> Self {
        ctx.register_key_getter_for::<DataTransferCompleted>(|e| e.dt.id as u64);
        ctx.register_key_getter_for::<DataWriteCompleted>(|e| e.request_id);
        ctx.register_key_getter_for::<DataWriteFailed>(|e| e.request_id);
        ctx.register_key_getter_for::<DownloadInputFileCompleted>(|e| e.workunit_id);

        Self {
            server_id: 0,
            net,
            disk,
            input_files: RefCell::new(HashMap::new()),
            output_files: HashMap::new(),
            ctx,
        }
    }

    pub fn set_server_id(&mut self, server_id: Id) {
        self.server_id = server_id;
    }

    pub fn download_file(&self, input_file: InputFileMetadata, from: Id) {
        self.ctx.spawn(self.process_download_file(input_file, from));
    }

    async fn process_download_file(&self, input_file: InputFileMetadata, from: Id) {
        let workunit_id = input_file.workunit_id;

        futures::join!(
            self.process_network_download(input_file.clone(), from),
            self.process_disk_write(input_file)
        );

        self.ctx
            .emit_now(DownloadInputFileCompleted { workunit_id }, self.server_id);
    }

    async fn process_network_download(&self, input_file: InputFileMetadata, from: Id) {
        let transfer_id = self.net.borrow_mut().transfer_data(
            from,
            self.ctx.id(),
            input_file.size as f64,
            self.ctx.id(),
        );

        self.ctx
            .recv_event_by_key::<DataTransferCompleted>(transfer_id as u64)
            .await;

        log_debug!(
            self.ctx,
            "received a new input file for workunit {}",
            transfer_id,
        );
    }

    async fn process_disk_write(&self, input_file: InputFileMetadata) {
        let workunit_id = input_file.workunit_id;
        // disk write
        let disk_write_id = self.disk.borrow_mut().write(input_file.size, self.ctx.id());

        select! {
            _ = self.ctx.recv_event_by_key::<DataWriteCompleted>(disk_write_id).fuse() => {
                log_debug!(self.ctx, "write completed!!! for workunit {}", workunit_id);
                self.input_files
                    .borrow_mut()
                    .insert(workunit_id, input_file);
            }
            _ = self.ctx.recv_event_by_key::<DataWriteFailed>(disk_write_id).fuse() => {
                log_debug!(self.ctx, "write FAILED!!! for workunit {}", workunit_id);
            }
        };
    }

    pub fn on_new_output_file(&mut self, output_file: OutputFileMetadata, client_id: Id) {
        log_debug!(
            self.ctx,
            "received new output file with id {} from client {}",
            output_file.id,
            client_id
        );
        self.output_files.insert(output_file.result_id, output_file);
    }

    pub fn delete_input_files(&mut self, workunit_id: WorkunitId) -> u32 {
        self.input_files.borrow_mut().remove(&workunit_id);
        // add disk free

        log_debug!(self.ctx, "deleted input files for workunit {}", workunit_id,);

        return 0;
    }

    pub fn delete_output_files(&mut self, result_id: ResultId) -> u32 {
        self.output_files.remove(&result_id);
        // add disk free

        log_debug!(self.ctx, "deleted output files for result {}", result_id,);

        return 0;
    }
}

impl EventHandler for DataServer {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            OutputFileFromClient { output_file } => {
                self.on_new_output_file(output_file, event.src);
            }
        })
    }
}
