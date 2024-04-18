use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_compute::multicore::*;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug, EventId};
use dslab_network::Network;
use dslab_storage::disk::Disk;
use dslab_storage::events::{
    DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed,
};
use dslab_storage::storage::Storage;
use futures::{select, FutureExt};

use super::scheduler::Scheduler;
use super::storage::FileStorage;
use super::task::{ResultInfo, ResultState};
use crate::common::Start;
use crate::server::data_server::{
    InputFileUploadCompleted, InputFilesInquiry, OutputFileDownloadCompleted, OutputFileFromClient,
};
use crate::server::job::{DataServerFile, JobSpec, ResultId, ResultRequest};
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
    pub result_id: ResultId,
}

#[derive(Clone, Serialize)]
pub struct ExecuteResult {
    pub result_id: ResultId,
}

#[derive(Clone, Serialize)]
pub struct ContinueResult {
    pub result_id: ResultId,
    pub comp_id: EventId,
}

#[derive(Clone, Serialize)]
pub struct ScheduleResults {}

// on result_request ->
// (ds) download input_file (start data transfer) ->
// download complete ->
// start calculation ->
// calculation finished ->
// (ds) upload output_file (start data transfer, emit_now(ds, output_file metadata)) ->
// output file uploaded ->
//ask for work

pub struct Client {
    compute: Rc<RefCell<Compute>>,
    disk: Rc<RefCell<Disk>>,
    net: Rc<RefCell<Network>>,
    server_id: Option<Id>,
    data_server_id: Option<Id>,
    scheduler: Scheduler,
    file_storage: Rc<FileStorage>,
    pub ctx: SimulationContext,
}

impl Client {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        disk: Rc<RefCell<Disk>>,
        net: Rc<RefCell<Network>>,
        mut scheduler: Scheduler,
        file_storage: Rc<FileStorage>,
        ctx: SimulationContext,
    ) -> Self {
        ctx.register_key_getter_for::<CompStarted>(|e| e.id);
        ctx.register_key_getter_for::<CompFinished>(|e| e.id);

        scheduler.set_client_id(ctx.id());
        Self {
            compute,
            disk,
            net,
            server_id: None,
            data_server_id: None,
            scheduler,
            file_storage,
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
        self.ctx.emit_self(ScheduleResults {}, 200.);
    }

    fn on_result_request(&self, req: ResultRequest, event_id: EventId) {
        self.ctx.spawn(self.process_result_request(req, event_id));
    }

    async fn process_result_request(&self, req: ResultRequest, event_id: EventId) {
        let mut result = ResultInfo {
            spec: req.spec,
            report_deadline: req.report_deadline,
            output_file: req.output_file,
            state: ResultState::Downloading,
            comp_id: None,
        };
        log_debug!(self.ctx, "job spec {:?}", result.spec);

        let workunit_id = result.spec.input_file.workunit_id;

        let input_files_already_downloaded = self
            .file_storage
            .input_files
            .borrow()
            .contains_key(&workunit_id);

        if !input_files_already_downloaded {
            self.ctx.emit_now(
                InputFilesInquiry {
                    workunit_id,
                    ref_id: event_id,
                },
                self.data_server_id.unwrap(),
            );

            futures::join!(
                self.process_data_server_input_file_download(event_id),
                self.process_disk_write(DataServerFile::Input(result.spec.input_file.clone())),
            );

            self.file_storage
                .input_files
                .borrow_mut()
                .insert(workunit_id, result.spec.input_file.clone());
        }

        result.state = ResultState::ReadyToExecute;

        self.file_storage
            .results
            .borrow_mut()
            .insert(result.spec.id, result);

        //self.ctx.emit_self_now(ScheduleResults {});
    }

    pub fn schedule_results(&self) {
        if self.scheduler.schedule() {
            self.ctx.emit_self(ScheduleResults {}, 200.);
        }
    }

    pub fn on_run_result(&self, result_id: ResultId) {
        self.ctx.spawn(self.run_result(result_id));
    }

    pub async fn run_result(&self, result_id: ResultId) {
        let result = self
            .file_storage
            .results
            .borrow_mut()
            .get_mut(&result_id)
            .unwrap()
            .clone();

        // disk read
        self.process_disk_read(DataServerFile::Input(result.spec.input_file.clone()))
            .await;

        log_debug!(
            self.ctx,
            "result {}: input files disk reading finished",
            result_id
        );

        log_debug!(self.ctx, "result {}: execution started", result_id);

        // comp start & update state
        self.process_compute(result_id, result.spec.clone()).await;

        log_debug!(self.ctx, "result {}: computing finished", result_id);

        // disk write
        self.process_disk_write(DataServerFile::Output(result.output_file.clone()))
            .await;

        log_debug!(
            self.ctx,
            "result {}: output files written on disk",
            result_id
        );

        // upload results on data server
        self.change_result(result_id, Some(ResultState::ReadyToUpload), None);
        self.ctx.emit_now(
            OutputFileFromClient {
                output_file: result.output_file,
            },
            self.data_server_id.unwrap(),
        );
        self.process_data_server_output_file_upload(result_id).await;

        log_debug!(
            self.ctx,
            "result {}: output files uploaded to data server",
            result_id
        );

        self.change_result(result_id, Some(ResultState::ReadyToNotify), None);
        self.net.borrow_mut().send_event(
            ResultCompleted { result_id },
            self.ctx.id(),
            self.server_id.unwrap(),
        );

        log_debug!(
            self.ctx,
            "result {}: server is notified about completion",
            result_id
        );

        self.change_result(result_id, Some(ResultState::Over), None);
        self.ask_for_results();
    }

    pub fn change_result(
        &self,
        result_id: ResultId,
        state: Option<ResultState>,
        comp_id: Option<EventId>,
    ) {
        let mut fs_results = self.file_storage.results.borrow_mut();
        let result = fs_results.get_mut(&result_id).unwrap();
        log_debug!(
            self.ctx,
            "Result {} state: {:?} -> {:?}",
            result_id,
            result.state,
            state
        );
        if state.is_some() {
            result.state = state.unwrap();
        }
        if comp_id.is_some() {
            result.comp_id = comp_id;
        }
    }

    async fn process_data_server_input_file_download(&self, ref_id: EventId) {
        self.ctx
            .recv_event_by_key::<InputFileUploadCompleted>(ref_id)
            .await;
    }

    async fn process_data_server_output_file_upload(&self, result_id: ResultId) {
        self.ctx
            .recv_event_by_key::<OutputFileDownloadCompleted>(result_id)
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

    async fn process_compute(&self, result_id: ResultId, spec: JobSpec) {
        let comp_id = self.compute.borrow_mut().run(
            spec.flops,
            spec.memory,
            spec.min_cores,
            spec.max_cores,
            spec.cores_dependency,
            self.ctx.id(),
        );

        self.ctx.recv_event_by_key::<CompStarted>(comp_id).await;
        log_debug!(self.ctx, "started execution of task: {}", spec.id);

        self.change_result(result_id, Some(ResultState::Running), Some(comp_id));
        self.file_storage
            .running_results
            .borrow_mut()
            .insert(result_id);

        self.ctx.recv_event_by_key::<CompFinished>(comp_id).await;
        log_debug!(self.ctx, "completed execution of task: {}", spec.id);

        self.file_storage
            .running_results
            .borrow_mut()
            .remove(&result_id);
    }

    pub fn on_continue_result(&self, result_id: ResultId, comp_id: EventId) {
        self.compute.borrow_mut().continue_computation(comp_id);
        self.file_storage
            .running_results
            .borrow_mut()
            .insert(result_id);
        log_debug!(self.ctx, "continue result: {}", result_id);
        self.change_result(result_id, Some(ResultState::Running), None);
    }

    fn ask_for_results(&self) {
        self.net
            .borrow_mut()
            .send_event(ResultsInquiry {}, self.ctx.id(), self.server_id.unwrap());
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
            ResultRequest {
                spec,
                report_deadline,
                output_file,
            } => {
                self.on_result_request(
                    ResultRequest {
                        spec,
                        report_deadline,
                        output_file,
                    },
                    event.id,
                );
            }
            ExecuteResult { result_id } => {
                self.on_run_result(result_id);
            }
            ContinueResult { result_id, comp_id } => {
                self.on_continue_result(result_id, comp_id);
            }
            ScheduleResults {} => {
                self.schedule_results();
            }
        })
    }
}
