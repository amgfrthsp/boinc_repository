use dslab_core::context::SimulationContext;
use dslab_core::{cast, log_info, Event, EventHandler};
use dslab_core::{component::Id, log_debug};
use dslab_network::{DataTransfer, DataTransferCompleted, Network};
use dslab_storage::disk::Disk;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

pub struct InputFileMetadata {
    pub id: u64,
    pub workunit_id: u64,
    pub nbytes: u64,
}

pub struct OutputFileMetadata {
    pub id: u64,
    pub max_nbytes: u64,
}

pub struct DataServer {
    id: Id,
    net: Rc<RefCell<Network>>,
    file_storage: Rc<RefCell<Disk>>,
    input_files: HashMap<u64, InputFileMetadata>,
    output_files: HashMap<u64, OutputFileMetadata>,
    downloads: HashMap<usize, u64>,
    uploads: HashMap<usize, u64>,
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
            uploads: HashMap::new(),
            ctx,
        };
    }

    pub fn load_new_input_file(&mut self, input_file: InputFileMetadata, from: u32) {
        let transfer_id =
            self.net
                .borrow_mut()
                .transfer_data(from, self.id, input_file.nbytes as f64, self.id);

        self.downloads.insert(transfer_id, input_file.id);
        self.input_files.insert(input_file.id, input_file);
    }

    pub fn load_new_output_file(&mut self, output_file: OutputFileMetadata, from: u32) {}

    fn on_data_transfer_completed(&mut self, dt: DataTransfer) {
        // data transfer corresponds to input download from job_generator
        let transfer_id = dt.id;
        if self.downloads.contains_key(&transfer_id) {
            let file_id = self.downloads.remove(&transfer_id).unwrap();
            let file_info = self.input_files.get(&file_id).unwrap();
            log_debug!(
                self.ctx,
                "received a new input file with id {} for workunit {}",
                file_id,
                file_info.workunit_id
            );
        } else {
            log_debug!(self.ctx, "received new output file");
        }
    }
}

impl EventHandler for DataServer {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataTransferCompleted { dt } => {
                self.on_data_transfer_completed(dt);
            }
        })
    }
}
