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

use super::task::{TaskInfo, TaskRequest, TaskState};
use crate::common::Start;
use crate::server::data_server::OutputFileFromClient;
use crate::simulator::simulator::SetServerIds;

#[derive(Clone, Serialize)]
pub struct TasksInquiry {}

#[derive(Clone, Serialize)]
pub struct ClientRegister {
    pub(crate) speed: f64,
    pub(crate) cpus_total: u32,
    pub(crate) memory_total: u64,
}

#[derive(Clone, Serialize)]
pub struct TaskCompleted {
    pub(crate) id: u64,
}

// on task_request ->
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
    tasks: HashMap<u64, TaskInfo>,
    computations: HashMap<u64, u64>,
    reads: HashMap<u64, u64>,
    writes: HashMap<u64, u64>,
    downloads: HashMap<usize, u64>,
    uploads: HashMap<usize, u64>,
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
            tasks: HashMap::new(),
            computations: HashMap::new(),
            reads: HashMap::new(),
            writes: HashMap::new(),
            downloads: HashMap::new(),
            uploads: HashMap::new(),
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

    fn on_task_request(&mut self, req: TaskRequest) {
        let task = TaskInfo {
            spec: req.spec,
            output_file: req.output_file,
            state: TaskState::Downloading,
        };
        log_debug!(self.ctx, "task spec: {:?}", task.spec);
        let transfer_id = self.net.borrow_mut().transfer_data(
            self.data_server_id.unwrap(),
            self.id,
            task.spec.input_file.size as f64,
            self.id,
        );
        self.downloads.insert(transfer_id, task.spec.id);
        self.tasks.insert(task.spec.id, task);
    }

    fn on_data_transfer_completed(&mut self, dt: DataTransfer) {
        // data transfer corresponds to input download
        let transfer_id = dt.id;
        if self.downloads.contains_key(&transfer_id) {
            let task_id = self.downloads.remove(&transfer_id).unwrap();
            let task = self.tasks.get_mut(&task_id).unwrap();
            log_debug!(self.ctx, "downloaded input data for task: {}", task_id);
            task.state = TaskState::Reading;
            let read_id = self
                .disk
                .borrow_mut()
                .read(task.spec.input_file.size, self.id);
            self.reads.insert(read_id, task_id);
        // data transfer corresponds to output upload
        } else if self.uploads.contains_key(&transfer_id) {
            let task_id = self.uploads.remove(&transfer_id).unwrap();
            let mut task = self.tasks.remove(&task_id).unwrap();
            log_debug!(self.ctx, "uploaded output data for task: {}", task_id);
            task.state = TaskState::Completed;
            self.disk
                .borrow_mut()
                .mark_free(task.output_file.size)
                .expect("Failed to free disk space");

            self.ctx.emit_now(
                OutputFileFromClient {
                    output_file: task.output_file,
                },
                self.data_server_id.unwrap(),
            );
            self.net.borrow_mut().send_event(
                TaskCompleted { id: task_id },
                self.id,
                self.server_id.unwrap(),
            );
            self.ask_for_tasks();
        }
    }

    fn on_data_read_completed(&mut self, request_id: u64) {
        let task_id = self.reads.remove(&request_id).unwrap();
        log_debug!(self.ctx, "read input data for task: {}", task_id);
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.state = TaskState::Running;
        let comp_id = self.compute.borrow_mut().run(
            task.spec.flops,
            task.spec.memory,
            task.spec.min_cores,
            task.spec.max_cores,
            task.spec.cores_dependency,
            self.id,
        );
        self.computations.insert(comp_id, task_id);
    }

    fn on_comp_started(&mut self, comp_id: u64) {
        let task_id = self.computations.get(&comp_id).unwrap();
        log_debug!(self.ctx, "started execution of task: {}", task_id);
    }

    fn on_comp_finished(&mut self, comp_id: u64) {
        let task_id = self.computations.remove(&comp_id).unwrap();
        log_debug!(self.ctx, "completed execution of task: {}", task_id);
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.state = TaskState::Writing;
        let write_id = self.disk.borrow_mut().write(task.output_file.size, self.id);
        self.writes.insert(write_id, task_id);
    }

    // Uploading results of completed tasks to server
    fn on_data_write_completed(&mut self, request_id: u64) {
        let task_id = self.writes.remove(&request_id).unwrap();
        log_debug!(self.ctx, "wrote output data for task: {}", task_id);
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.state = TaskState::Uploading;
        let transfer_id = self.net.borrow_mut().transfer_data(
            self.id,
            self.server_id.unwrap(),
            task.output_file.size as f64,
            self.id,
        );
        self.uploads.insert(transfer_id, task_id);
    }

    fn ask_for_tasks(&mut self) {
        self.net
            .borrow_mut()
            .send_event(TasksInquiry {}, self.id, self.server_id.unwrap());
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
            TaskRequest { spec, output_file } => {
                self.on_task_request(TaskRequest { spec, output_file });
            }
            DataTransferCompleted { dt } => {
                self.on_data_transfer_completed(dt);
            }
            DataReadCompleted {
                request_id,
                size: _,
            } => {
                self.on_data_read_completed(request_id);
            }
            CompStarted { id, cores: _ } => {
                self.on_comp_started(id);
            }
            CompFinished { id } => {
                self.on_comp_finished(id);
            }
            DataWriteCompleted {
                request_id,
                size: _,
            } => {
                self.on_data_write_completed(request_id);
            }
        })
    }
}
