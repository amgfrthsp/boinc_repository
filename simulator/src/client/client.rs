use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde::Serialize;

use dslab_compute::multicore::*;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug};
use dslab_network::{DataTransfer, DataTransferCompleted, Network};
use dslab_storage::disk::Disk;
use dslab_storage::events::{DataReadCompleted, DataWriteCompleted};
use dslab_storage::storage::Storage;

use super::task::{ClientResultState, ResultInfo};
use crate::common::Start;
use crate::server::data_server::{
    InputFileUploadCompleted, InputFilesInquiry, OutputFileFromClient,
};
use crate::server::job::ResultRequest;
use crate::simulator::simulator::SetServerIds;

#[derive(Clone, Serialize)]
pub struct ResultsInquiry {}

#[derive(Clone, Serialize)]
pub struct ClientRegister {
    pub speed: f64,
    pub cpus_total: u32,
    pub memory_total: u64,
}

#[derive(Clone, Serialize)]
pub struct ResultCompleted {
    pub id: u64,
}

// on result_request ->
// (ds) download input_file (start data transfer) ->
// download complete ->
// start calculation ->
// calculation finished ->
// (ds) upload output_file (start data transfer, emit_now(ds, output_file metadata)) ->
// output file uploaded ->
//ask for work

// thoughts:
// 1. finished data transfer events should occur on the client side so that in case of
// failure client is the one who initiates retries
// 2. we should be able to differentiate between downloads and uploads to be able to do retries
// 3. who initiates data transfers?

pub struct Client {
    id: Id,
    compute: Rc<RefCell<Compute>>,
    disk: Rc<RefCell<Disk>>,
    net: Rc<RefCell<Network>>,
    server_id: Option<Id>,
    data_server_id: Option<Id>,
    results: RefCell<HashMap<u64, ResultInfo>>,
    computations: HashMap<u64, u64>,
    reads: HashMap<u64, u64>,
    writes: HashMap<u64, u64>,
    pub ctx: SimulationContext,
}

impl Client {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        disk: Rc<RefCell<Disk>>,
        net: Rc<RefCell<Network>>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id(),
            compute,
            disk,
            net,
            server_id: None,
            data_server_id: None,
            results: RefCell::new(HashMap::new()),
            computations: HashMap::new(),
            reads: HashMap::new(),
            writes: HashMap::new(),
            ctx,
        }
    }

    fn on_start(&mut self) {
        log_debug!(self.ctx, "started");
        self.ctx.emit(
            ClientRegister {
                speed: self.compute.borrow().speed(),
                cpus_total: self.compute.borrow().cores_total(),
                memory_total: self.compute.borrow().memory_total(),
            },
            self.server_id.unwrap(),
            0.5,
        );
    }

    async fn on_result_request(&self, req: ResultRequest) {
        let result = ResultInfo {
            spec: req.spec,
            output_file: req.output_file,
            state: ClientResultState::Downloading,
        };
        log_debug!(self.ctx, "job spec {:?}", result.spec);

        let workunit_id = result.spec.input_file.workunit_id;

        self.ctx.emit_now(
            InputFilesInquiry { workunit_id },
            self.data_server_id.unwrap(),
        );

        self.ctx
            .recv_event_by_key::<InputFileUploadCompleted>(workunit_id)
            .await;

        self.results.borrow_mut().insert(result.spec.id, result);
    }

    // fn on_comp_started(&mut self, comp_id: u64) {
    //     let result_id = self.computations.get(&comp_id).unwrap();
    //     log_debug!(self.ctx, "started execution of result {}", result_id);
    // }

    // fn on_comp_finished(&mut self, comp_id: u64) {
    //     let result_id = self.computations.remove(&comp_id).unwrap();
    //     log_debug!(self.ctx, "completed execution of result {}", result_id);
    //     let result = self.results.get_mut(&result_id).unwrap();
    //     result.state = ClientResultState::Writing;
    //     let write_id = self
    //         .disk
    //         .borrow_mut()
    //         .write(result.output_file.size, self.id);
    //     self.writes.insert(write_id, result_id);
    // }

    // // Uploading results of completed results to server
    // fn on_data_write_completed(&mut self, request_id: u64) {
    //     let result_id = self.writes.remove(&request_id).unwrap();
    //     log_debug!(self.ctx, "wrote output data for result {}", result_id);
    //     let result = self.results.get_mut(&result_id).unwrap();
    //     result.state = ClientResultState::Uploading;
    //     let transfer_id = self.net.borrow_mut().transfer_data(
    //         self.id,
    //         self.server_id.unwrap(),
    //         result.output_file.size as f64,
    //         self.id,
    //     );
    //     self.uploads.insert(transfer_id, result_id);
    // }

    fn ask_for_results(&mut self) {
        self.net
            .borrow_mut()
            .send_event(ResultsInquiry {}, self.id, self.server_id.unwrap());
    }
}

impl EventHandler for Client {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            SetServerIds {
                server_id,
                data_server_id,
            } => {
                self.server_id = Some(server_id);
                self.data_server_id = Some(data_server_id);
            }
            Start {} => {
                self.on_start();
            }
            ResultRequest { spec, output_file } => {
                self.on_result_request(ResultRequest { spec, output_file });
            }
            DataTransferCompleted { dt } => {
                //self.on_data_transfer_completed(dt);
            }
            DataReadCompleted {
                request_id,
                size: _,
            } => {
                //self.on_data_read_completed(request_id);
            }
            CompStarted { id, cores: _ } => {
                //self.on_comp_started(id);
            }
            CompFinished { id } => {
                //self.on_comp_finished(id);
            }
            DataWriteCompleted {
                request_id,
                size: _,
            } => {
                //self.on_data_write_completed(request_id);
            }
        })
    }
}
