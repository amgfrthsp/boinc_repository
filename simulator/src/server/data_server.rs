use dslab_core::context::SimulationContext;
use dslab_core::{cast, Event, EventHandler};
use dslab_core::{component::Id, log_debug};
use dslab_network::{DataTransfer, DataTransferCompleted, Network};
use dslab_storage::disk::Disk;
use serde::Serialize;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use super::job::{InputFileMetadata, OutputFileMetadata};

#[derive(Clone, Serialize)]
pub struct OutputFileFromClient {
    pub output_file: OutputFileMetadata,
}

pub struct DataServer {
    id: Id,
    net: Rc<RefCell<Network>>,
    file_storage: Rc<RefCell<Disk>>,
    input_files: HashMap<u64, InputFileMetadata>, // workunit_id -> input files
    output_files: HashMap<u64, OutputFileMetadata>, // result_id -> output files
    downloads: HashMap<usize, u64>,
    ctx: SimulationContext,
}

impl DataServer {
    pub fn new(
        net: Rc<RefCell<Network>>,
        file_storage: Rc<RefCell<Disk>>,
        ctx: SimulationContext,
    ) -> Self {
        return Self {
            id: ctx.id(),
            net,
            file_storage,
            input_files: HashMap::new(),
            output_files: HashMap::new(),
            downloads: HashMap::new(),
            ctx,
        };
    }

    pub fn load_input_file_for_workunit(&mut self, input_file: InputFileMetadata, from: u32) {
        let transfer_id =
            self.net
                .borrow_mut()
                .transfer_data(from, self.id, input_file.size as f64, self.id);

        self.downloads.insert(transfer_id, input_file.id);
        self.input_files.insert(input_file.workunit_id, input_file);
    }

    fn on_data_transfer_completed(&mut self, dt: DataTransfer) {
        // data transfer corresponds to input download from job_generator
        let file_id = self.downloads.remove(&dt.id).unwrap();
        log_debug!(
            self.ctx,
            "received a new input file for workunit {}",
            file_id
        );
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

    pub fn delete_input_files(&mut self, workunit_id: u64) -> u32 {
        self.input_files.remove(&workunit_id);
        // add disk free
        return 0;
    }

    pub fn delete_output_files(&mut self, result_id: u64) -> u32 {
        self.output_files.remove(&result_id);
        // add disk free
        return 0;
    }
}

impl EventHandler for DataServer {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataTransferCompleted { dt } => {
                self.on_data_transfer_completed(dt);
            }
            OutputFileFromClient { output_file } => {
                self.on_new_output_file(output_file, event.src);
            }
        })
    }
}
