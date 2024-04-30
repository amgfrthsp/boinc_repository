use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_compute::multicore::*;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug, log_info, EventId};
use dslab_network::Network;
use dslab_storage::disk::Disk;
use dslab_storage::events::{
    DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed,
};
use dslab_storage::storage::Storage;
use futures::{select, FutureExt};

use super::rr_simulation::RRSimulation;
use super::scheduler::Scheduler;
use super::storage::FileStorage;
use super::task::{ResultInfo, ResultState};
use crate::common::ReportStatus;
use crate::config::sim_config::ClientConfig;
use crate::server::data_server::{
    InputFileUploadCompleted, InputFilesInquiry, OutputFileDownloadCompleted, OutputFileFromClient,
};
use crate::server::job::{DataServerFile, JobSpec, ResultId, ResultRequest};
use crate::server::job_generator::AllJobsSent;
use crate::simulator::simulator::StartClient;

#[derive(Clone, Serialize, Debug)]
pub struct WorkFetchRequest {
    pub req_secs: f64,
    pub req_instances: i32,
    pub estimated_delay: f64,
}

#[derive(Clone, Serialize)]
pub struct WorkFetchReply {
    pub requests: Vec<ResultRequest>,
}

#[derive(Clone, Serialize)]
pub struct AskForWork {}

#[derive(Clone, Serialize)]
pub struct ClientRegister {
    pub speed: f64,
    pub cores: u32,
    pub memory: u64,
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

pub struct Client {
    compute: Rc<RefCell<Compute>>,
    disk: Rc<RefCell<Disk>>,
    net: Rc<RefCell<Network>>,
    server_id: Id,
    data_server_id: Id,
    scheduler: Scheduler,
    rr_sim: Rc<RefCell<RRSimulation>>,
    file_storage: Rc<FileStorage>,
    next_scheduling_time: RefCell<f64>,
    scheduling_event: RefCell<Option<EventId>>,
    received_all_jobs: bool,
    pub ctx: SimulationContext,
    config: ClientConfig,
}

impl Client {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        disk: Rc<RefCell<Disk>>,
        net: Rc<RefCell<Network>>,
        mut scheduler: Scheduler,
        rr_sim: Rc<RefCell<RRSimulation>>,
        file_storage: Rc<FileStorage>,
        ctx: SimulationContext,
        config: ClientConfig,
    ) -> Self {
        ctx.register_key_getter_for::<CompStarted>(|e| e.id);
        ctx.register_key_getter_for::<CompFinished>(|e| e.id);

        scheduler.set_client_id(ctx.id());
        Self {
            compute,
            disk,
            net,
            server_id: 0,
            data_server_id: 0,
            scheduler,
            rr_sim,
            file_storage,
            next_scheduling_time: RefCell::new(0.),
            scheduling_event: RefCell::new(None),
            received_all_jobs: false,
            ctx,
            config,
        }
    }

    fn on_start(&mut self, server_id: Id, data_server_id: Id) {
        log_debug!(self.ctx, "started");
        self.server_id = server_id;
        self.data_server_id = data_server_id;
        self.ctx.emit(
            ClientRegister {
                speed: self.compute.borrow().speed(),
                cores: self.compute.borrow().cores_total(),
                memory: self.compute.borrow().memory_total(),
            },
            self.server_id,
            0.5,
        );
        self.ctx
            .emit_self(ReportStatus {}, self.config.report_status_interval);
        self.ctx.emit_self(AskForWork {}, 65.);
    }

    fn on_result_requests(&self, reqs: Vec<ResultRequest>) {
        for req in reqs {
            self.ctx.spawn(self.process_result_request(req));
        }
    }

    async fn process_result_request(&self, req: ResultRequest) {
        let ref_id = req.spec.id;
        let mut result = ResultInfo {
            spec: req.spec,
            report_deadline: req.report_deadline,
            output_file: req.output_file,
            state: ResultState::Downloading,
            time_added: self.ctx.time(),
            comp_id: None,
            sim_miss_deadline: false,
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
                    ref_id,
                },
                self.data_server_id,
            );

            futures::join!(
                self.process_data_server_input_file_download(ref_id),
                self.process_disk_write(DataServerFile::Input(result.spec.input_file.clone())),
            );

            self.file_storage
                .input_files
                .borrow_mut()
                .insert(workunit_id, result.spec.input_file.clone());
        }

        result.state = ResultState::Unstarted;

        self.file_storage
            .results
            .borrow_mut()
            .insert(result.spec.id, result);

        self.plan_scheduling(5.);
    }

    pub fn plan_scheduling(&self, delay: f64) {
        let planned = self.scheduling_event.borrow().is_some();
        if planned {
            if *self.next_scheduling_time.borrow() <= self.ctx.time() + delay {
                return;
            } else {
                self.ctx
                    .cancel_event(self.scheduling_event.borrow().unwrap());
            }
        }
        log_debug!(
            self.ctx,
            "Planned scheduling for {}",
            self.ctx.time() + delay
        );
        *self.scheduling_event.borrow_mut() = Some(self.ctx.emit_self(ScheduleResults {}, delay));
        *self.next_scheduling_time.borrow_mut() = self.ctx.time() + delay;
    }

    pub fn schedule_results(&self) {
        self.scheduler.schedule();
        *self.scheduling_event.borrow_mut() = None;
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

        self.change_result(result_id, Some(ResultState::Reading), None);

        // disk read
        self.process_disk_read(DataServerFile::Input(result.spec.input_file.clone()))
            .await;
        log_debug!(
            self.ctx,
            "result {}: input files disk reading finished",
            result_id
        );
        if result.state == ResultState::Canceled {
            self.change_result(result_id, Some(ResultState::Unstarted), None);
            return;
        }

        // comp start & update state
        self.process_compute(result_id, result.spec.clone()).await;

        // disk write
        self.change_result(result_id, Some(ResultState::Writing), None);
        self.process_disk_write(DataServerFile::Output(result.output_file.clone()))
            .await;
        self.file_storage
            .output_files
            .borrow_mut()
            .insert(result.output_file.result_id, result.output_file.clone());
        log_debug!(
            self.ctx,
            "result {}: output files written on disk",
            result_id
        );

        // upload results on data server
        self.change_result(result_id, Some(ResultState::Uploading), None);
        self.ctx.emit_now(
            OutputFileFromClient {
                output_file: result.output_file.clone(),
            },
            self.data_server_id,
        );
        self.process_data_server_output_file_upload(result_id).await;

        log_debug!(
            self.ctx,
            "result {}: output files uploaded to data server",
            result_id
        );

        self.change_result(result_id, Some(ResultState::Notifying), None);
        self.net.borrow_mut().send_event(
            ResultCompleted { result_id },
            self.ctx.id(),
            self.server_id,
        );
        log_debug!(
            self.ctx,
            "result {}: server is notified about completion",
            result_id
        );

        self.change_result(result_id, Some(ResultState::Over), None);
        self.plan_scheduling(5.);

        self.process_disk_free(DataServerFile::Input(result.spec.input_file));
        self.process_disk_free(DataServerFile::Output(result.output_file));
        log_debug!(
            self.ctx,
            "result {}: deleted input/output files from disk",
            result_id
        );
        self.change_result(result_id, Some(ResultState::Deleted), None);
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

    fn process_disk_free(&self, file: DataServerFile) {
        let res = self.disk.borrow_mut().mark_free(file.size());

        if res.is_ok() {
            match file {
                DataServerFile::Input(input_file) => {
                    self.file_storage
                        .input_files
                        .borrow_mut()
                        .remove(&input_file.workunit_id);
                }
                DataServerFile::Output(output_file) => {
                    self.file_storage
                        .output_files
                        .borrow_mut()
                        .remove(&output_file.result_id);
                }
            }
        }
    }

    async fn process_compute(&self, result_id: ResultId, spec: JobSpec) {
        let comp_id = self.compute.borrow_mut().run(
            spec.flops,
            spec.memory,
            spec.cores,
            spec.cores,
            spec.cores_dependency,
            self.ctx.id(),
        );

        self.ctx.recv_event_by_key::<CompStarted>(comp_id).await;
        log_debug!(self.ctx, "result {}: execution started", spec.id);

        self.change_result(result_id, Some(ResultState::Running), Some(comp_id));
        self.file_storage
            .running_results
            .borrow_mut()
            .insert(result_id);

        self.ctx.recv_event_by_key::<CompFinished>(comp_id).await;
        log_debug!(self.ctx, "result {}: execution finished", spec.id);

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

    fn on_work_fetch(&self) {
        let sim_result = self.rr_sim.borrow().simulate(false);

        if sim_result.work_fetch_req.estimated_delay < self.config.buffered_work_lower_bound {
            self.net.borrow_mut().send_event(
                sim_result.work_fetch_req,
                self.ctx.id(),
                self.server_id,
            );
        }

        if self.is_active() {
            self.ctx
                .emit_self(AskForWork {}, self.config.work_fetch_interval);
        }
    }

    fn is_active(&self) -> bool {
        return !(self.received_all_jobs && self.file_storage.running_results.borrow().is_empty());
    }

    fn report_status(&mut self) {
        log_info!(
            self.ctx,
            "CPU: {:.2} / MEMORY: {:.2} / DISK: {:.2}",
            (self.compute.borrow().cores_total() - self.compute.borrow().cores_available()) as f64
                / self.compute.borrow().cores_total() as f64,
            (self.compute.borrow().memory_total() - self.compute.borrow().memory_available())
                as f64
                / self.compute.borrow().memory_total() as f64,
            self.disk.borrow().used_space() as f64 / self.disk.borrow().capacity() as f64
        );
        let fs_ref = self.file_storage.results.borrow();
        for result in fs_ref.values() {
            log_info!(self.ctx, "Result {}: {:?}", result.spec.id, result.state);
        }
        if self.is_active() {
            self.ctx
                .emit_self(ReportStatus {}, self.config.report_status_interval);
        }
    }
}

impl EventHandler for Client {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            StartClient {
                server_id,
                data_server_id,
            } => {
                self.on_start(server_id, data_server_id);
            }
            WorkFetchReply { requests } => {
                self.on_result_requests(requests);
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
            AskForWork {} => {
                self.on_work_fetch();
            }
            ReportStatus {} => {
                self.report_status();
            }
            AllJobsSent {} => {
                self.received_all_jobs = true;
            }
        })
    }
}
