use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;

use serde::Serialize;

use dslab_compute::multicore::*;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::StaticEventHandler;
use dslab_core::{cast, log_debug, log_info, EventId};
use dslab_network::Network;
use dslab_storage::disk::Disk;
use dslab_storage::events::{
    DataReadCompleted, DataReadFailed, DataWriteCompleted, DataWriteFailed,
};
use dslab_storage::storage::Storage;
use futures::{select, FutureExt};

use super::rr_simulation::{RRSimulation, RRSimulationResult};
use super::stats::ClientStats;
use super::storage::FileStorage;
use super::task::{ResultInfo, ResultState};
use super::utils::Utilities;
use crate::common::{Finish, ReportStatus};
use crate::config::sim_config::ClientGroupConfig;
use crate::project::data_server::{
    InputFileUploadCompleted, InputFilesInquiry, OutputFileDownloadCompleted, OutputFileFromClient,
};
use crate::project::job::{DataServerFile, JobSpec, ResultId, ResultRequest};
use crate::simulator::dist_params::SimulationDistribution;
use crate::simulator::simulator::StartClient;

// 1 credit is given for processing GFLOPS_CREDIT_RATIO GFLOPS
pub const GFLOPS_CREDIT_RATIO: f64 = 24. * 60. * 60. / 200.;

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
pub struct Suspend {}

#[derive(Clone, Serialize)]
pub struct Resume {}

#[derive(Clone, Serialize)]
pub struct ClientRegister {
    pub speed: f64,
    pub cores: u32,
    pub memory: u64,
}

#[derive(Clone, Serialize)]
pub struct ProjectInfo {
    pub server_id: Id,
    pub data_server_id: Id,
    pub resource_share: f64,
    pub rec: f64,
}

impl ProjectInfo {
    pub fn new(server_id: Id, data_server_id: Id, resource_share: f64) -> Self {
        Self {
            server_id,
            data_server_id,
            resource_share,
            rec: 0.,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct ResultCompleted {
    pub result_id: ResultId,
    pub is_correct: bool,
    pub claimed_credit: f64,
}

#[derive(Clone, Serialize)]
pub struct ScheduleResults {}

pub struct Client {
    pub compute: Rc<RefCell<Compute>>,
    pub disk: Rc<RefCell<Disk>>,
    network: Rc<RefCell<Network>>,
    utilities: Rc<RefCell<Utilities>>,
    projects: RefCell<HashMap<Id, ProjectInfo>>, // server_id -> project
    pub rr_sim: Rc<RefCell<RRSimulation>>,
    file_storage: Rc<FileStorage>,
    next_scheduling_time: RefCell<f64>,
    scheduling_event: RefCell<Option<EventId>>,
    suspended: RefCell<bool>,
    pub ctx: SimulationContext,
    pub config: ClientGroupConfig,
    reliability: f64,
    av_distribution: SimulationDistribution,
    unav_distribution: SimulationDistribution,
    is_active: RefCell<bool>,
    finish_time: RefCell<f64>,
    pub stats: Rc<RefCell<ClientStats>>,
}

impl Client {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        disk: Rc<RefCell<Disk>>,
        network: Rc<RefCell<Network>>,
        projects: HashMap<Id, ProjectInfo>,
        utilities: Rc<RefCell<Utilities>>,
        rr_sim: Rc<RefCell<RRSimulation>>,
        file_storage: Rc<FileStorage>,
        ctx: SimulationContext,
        config: ClientGroupConfig,
        reliability: f64,
        av_distribution: SimulationDistribution,
        unav_distribution: SimulationDistribution,
        stats: Rc<RefCell<ClientStats>>,
    ) -> Self {
        ctx.register_key_getter_for::<CompStarted>(|e| e.id);
        ctx.register_key_getter_for::<CompFinished>(|e| e.id);

        Self {
            compute,
            disk,
            network,
            utilities,
            projects: RefCell::new(projects),
            rr_sim,
            file_storage,
            next_scheduling_time: RefCell::new(0.),
            scheduling_event: RefCell::new(None),
            suspended: RefCell::new(false),
            ctx,
            config,
            reliability,
            av_distribution,
            unav_distribution,
            is_active: RefCell::new(true),
            finish_time: RefCell::new(0.),
            stats,
        }
    }

    fn is_active(&self) -> bool {
        *self.is_active.borrow()
    }

    fn get_finish_time(&self) -> f64 {
        *self.finish_time.borrow()
    }

    fn is_suspended(&self) -> bool {
        *self.suspended.borrow()
    }

    fn on_start(&self, finish_time: f64) {
        log_info!(self.ctx, "started");
        *self.finish_time.borrow_mut() = finish_time;
        self.ctx.emit_self(Finish {}, finish_time);

        let projects_ref = self.projects.borrow();
        for (server_id, _) in projects_ref.iter() {
            self.ctx.emit(
                ClientRegister {
                    speed: self.compute.borrow().speed(),
                    cores: self.compute.borrow().cores_total(),
                    memory: self.compute.borrow().memory_total(),
                },
                *server_id,
                0.,
            );
        }
        self.ctx
            .emit_self(ReportStatus {}, self.config.report_status_interval);
        self.ctx
            .emit_self(AskForWork {}, self.ctx.gen_range(5. ..10.));

        let resume_dur = self.ctx.sample_from_distribution(&self.av_distribution) * 3600.;
        self.ctx.emit_self(Suspend {}, resume_dur);

        self.stats.borrow_mut().time_available +=
            if self.ctx.time() + resume_dur > self.get_finish_time() {
                self.get_finish_time() - self.ctx.time()
            } else {
                resume_dur
            };

        log_info!(
            self.ctx,
            "Client is started and is active for {}",
            resume_dur
        );
    }

    fn on_suspend(&self) {
        *self.suspended.borrow_mut() = true;
        let running_results = self.file_storage.running_results.borrow().clone();
        let mut fs_results = self.file_storage.results.borrow_mut();

        for result_id in running_results {
            let result = fs_results.get_mut(&result_id).unwrap();
            self.preempt_result(result);
        }

        let suspension_dur = self.ctx.sample_from_distribution(&self.unav_distribution) * 3600.;
        self.ctx.emit_self(Resume {}, suspension_dur);

        self.stats.borrow_mut().time_unavailable +=
            if self.ctx.time() + suspension_dur > self.get_finish_time() {
                self.get_finish_time() - self.ctx.time()
            } else {
                suspension_dur
            };

        log_info!(self.ctx, "Client suspended for {}s", suspension_dur);
    }

    fn on_resume(&self) {
        *self.suspended.borrow_mut() = false;

        self.ctx.emit_self(AskForWork {}, 0.);
        self.ctx.emit_self(ScheduleResults {}, 0.);

        let resume_dur = self.ctx.sample_from_distribution(&self.av_distribution) * 3600.;

        self.ctx.emit_self(Suspend {}, resume_dur);

        self.stats.borrow_mut().time_available +=
            if self.ctx.time() + resume_dur > self.get_finish_time() {
                self.get_finish_time() - self.ctx.time()
            } else {
                resume_dur
            };

        log_info!(self.ctx, "Client resumed for {}", resume_dur);
    }

    fn on_result_requests(self: Rc<Self>, reqs: Vec<ResultRequest>, server_id: Id) {
        for req in reqs {
            self.ctx
                .spawn(self.clone().process_result_request(req, server_id));
        }
    }

    async fn process_result_request(self: Rc<Self>, req: ResultRequest, server_id: Id) {
        let ref_id = req.spec.id;
        let projects_ref = self.projects.borrow();
        let project_info = projects_ref.get(&server_id).unwrap();

        let mut result = ResultInfo {
            server_id,
            spec: req.spec,
            report_deadline: req.report_deadline,
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
                project_info.data_server_id,
            );

            futures::join!(
                self.process_data_server_input_file_download(ref_id),
                self.process_disk_write(result.spec.input_file.size),
            );

            self.file_storage
                .input_files
                .borrow_mut()
                .insert(workunit_id, result.spec.input_file.clone());
        }

        result.state = ResultState::Unstarted;

        self.file_storage
            .results_for_sim
            .borrow_mut()
            .insert(result.spec.id);

        self.file_storage
            .results
            .borrow_mut()
            .insert(result.spec.id, result);

        self.plan_scheduling(3.);
    }

    pub fn plan_scheduling(&self, delay: f64) {
        if self.is_suspended() {
            return;
        }
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

    pub fn perform_rr_sim(&self, is_scheduling: bool) -> RRSimulationResult {
        let t = Instant::now();

        let sim_result = self.rr_sim.borrow_mut().simulate(is_scheduling);

        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().rrsim_sum_dur += duration;
        self.stats.borrow_mut().rrsim_samples += 1;

        sim_result
    }

    pub fn schedule_results(self: Rc<Self>) {
        let t = Instant::now();
        if self.is_suspended() {
            return;
        }
        log_info!(self.ctx, "scheduling started");

        let sim_result = self.perform_rr_sim(true);

        let results_to_schedule = sim_result.results_to_schedule;

        log_info!(
            self.ctx,
            "All results: {}; ready to schedule: {}; running: {}",
            self.file_storage.results.borrow().len(),
            results_to_schedule.len(),
            self.file_storage.running_results.borrow().len()
        );

        let mut cores_available = self.compute.borrow().cores_total();
        let mut memory_available = self.compute.borrow().memory_total();

        let mut scheduled_results: Vec<ResultId> = Vec::new();
        let mut cont_results: Vec<(ResultId, EventId)> = Vec::new();
        let mut start_results: Vec<ResultId> = Vec::new();

        let mut fs_results = self.file_storage.results.borrow_mut();

        let mut skip = 0;
        let mut cont = 0;
        let mut start = 0;
        let mut preempt = 0;

        for result_id in results_to_schedule {
            let result = fs_results.get_mut(&result_id).unwrap();
            if cores_available == 0 {
                break;
            }
            if result.spec.cores > cores_available || result.spec.memory > memory_available {
                log_debug!(self.ctx, "Skip result {}", result_id);
                skip += 1;
                continue;
            }
            cores_available -= result.spec.cores;
            memory_available -= result.spec.memory;

            if let ResultState::Preempted { comp_id } = result.state {
                log_debug!(self.ctx, "Continue result {}", result_id);
                cont_results.push((result_id, comp_id));
                cont += 1;
            } else if result.state == ResultState::Unstarted {
                log_debug!(self.ctx, "Start result {}", result_id);
                start_results.push(result_id);
                start += 1;
            } else {
                log_debug!(
                    self.ctx,
                    "Keep result {} with state {:?} running",
                    result_id,
                    result.state
                );
            }
            scheduled_results.push(result_id);
        }

        let clone = self.file_storage.running_results.borrow().clone();

        for result_id in clone {
            let result = fs_results.get_mut(&result_id).unwrap();
            if !scheduled_results.contains(&result_id)
                && !(result.state == ResultState::Running
                    && self.utilities.borrow().is_running_finished(result))
            {
                self.preempt_result(result);
                preempt += 1;
            }
        }

        drop(fs_results);

        for (result_id, comp_id) in cont_results {
            self.on_continue_result(result_id, comp_id);
        }
        for result_id in start_results {
            self.clone().on_run_result(result_id);
        }

        log_info!(
            self.ctx,
            "scheduling finished. skip {} continue {} start {} preempt {}",
            skip,
            cont,
            start,
            preempt
        );

        *self.scheduling_event.borrow_mut() = None;
        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().scheduler_sum_dur += duration;
        self.stats.borrow_mut().scheduler_samples += 1;
    }

    pub fn on_run_result(self: Rc<Self>, result_id: ResultId) {
        if self.is_suspended() {
            return;
        }
        self.ctx.spawn(self.clone().run_result(result_id));
    }

    pub async fn run_result(self: Rc<Self>, result_id: ResultId) {
        let result = self
            .file_storage
            .results
            .borrow_mut()
            .get_mut(&result_id)
            .unwrap()
            .clone();

        self.change_result(result_id, Some(ResultState::Reading), None);

        // disk read
        self.process_disk_read(result.spec.input_file.size).await;
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
        self.process_compute(result_id, &result.spec).await;

        // disk write
        self.change_result(result_id, Some(ResultState::Writing), None);
        self.process_disk_write(result.spec.output_file.size).await;
        self.file_storage.output_files.borrow_mut().insert(
            result.spec.output_file.result_id,
            result.spec.output_file.clone(),
        );
        log_debug!(
            self.ctx,
            "result {}: output files written on disk",
            result_id
        );

        // upload results on data server
        self.change_result(result_id, Some(ResultState::Uploading), None);

        let projects_ref = self.projects.borrow();
        let project_info = projects_ref.get(&result.server_id).unwrap();
        self.ctx.emit_now(
            OutputFileFromClient {
                output_file: result.spec.output_file.clone(),
            },
            project_info.data_server_id,
        );
        self.process_data_server_output_file_upload(result_id).await;

        log_debug!(
            self.ctx,
            "result {}: output files uploaded to data server",
            result_id
        );

        self.change_result(result_id, Some(ResultState::Notifying), None);

        let claimed_credit = result.spec.gflops / GFLOPS_CREDIT_RATIO;
        self.network.borrow_mut().send_event(
            ResultCompleted {
                result_id,
                is_correct: self.ctx.gen_range(0. ..1.) < self.reliability,
                claimed_credit,
            },
            self.ctx.id(),
            result.server_id,
        );
        log_debug!(
            self.ctx,
            "result {}: server is notified about completion",
            result_id
        );

        self.change_result(result_id, Some(ResultState::Over), None);
        self.plan_scheduling(3.);

        self.process_disk_free(DataServerFile::Input(result.spec.input_file));
        self.process_disk_free(DataServerFile::Output(result.spec.output_file));
        log_debug!(
            self.ctx,
            "result {}: deleted input/output files from disk",
            result_id
        );
        let processing_time = self.ctx.time() - result.time_added;
        self.change_result(result_id, Some(ResultState::Deleted), None);
        self.file_storage.results.borrow_mut().remove(&result_id);

        self.stats.borrow_mut().results_processing_time += processing_time;

        let min_processing_time = self.stats.borrow_mut().min_result_processing_time;
        self.stats.borrow_mut().min_result_processing_time =
            min_processing_time.min(processing_time);
        let max_processing_time = self.stats.borrow_mut().max_result_processing_time;
        self.stats.borrow_mut().max_result_processing_time =
            max_processing_time.max(processing_time);

        self.stats.borrow_mut().n_results_processed += 1;
        self.stats.borrow_mut().gflops_processed += result.spec.gflops;
        if self.ctx.time() > result.report_deadline {
            self.stats.borrow_mut().n_miss_deadline += 1;
        }
    }

    pub fn change_result(
        &self,
        result_id: ResultId,
        state: Option<ResultState>,
        comp_id: Option<EventId>,
    ) {
        let mut fs_results = self.file_storage.results.borrow_mut();
        let result = fs_results.get_mut(&result_id).unwrap();
        log_info!(
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

    async fn process_disk_write(&self, size: u64) {
        let disk_write_id = self.disk.borrow_mut().write(size, self.ctx.id());

        select! {
            _ = self.ctx.recv_event_by_key::<DataWriteCompleted>(disk_write_id).fuse() => {
                log_debug!(self.ctx, "write completed!!!");
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

    async fn process_compute(&self, result_id: ResultId, spec: &JobSpec) {
        let comp_id = self.compute.borrow_mut().run(
            spec.gflops,
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

        self.file_storage
            .results_for_sim
            .borrow_mut()
            .remove(&result_id);
    }

    pub fn on_continue_result(&self, result_id: ResultId, comp_id: EventId) {
        if self.is_suspended() {
            return;
        }
        self.compute.borrow_mut().continue_computation(comp_id);
        self.file_storage
            .running_results
            .borrow_mut()
            .insert(result_id);
        log_debug!(self.ctx, "continue result: {}", result_id);
        self.change_result(result_id, Some(ResultState::Running), None);
    }

    pub fn preempt_result(&self, result: &mut ResultInfo) {
        match result.state {
            ResultState::Running => {
                self.compute
                    .borrow_mut()
                    .preempt_computation(result.comp_id.unwrap());

                result.state = ResultState::Preempted {
                    comp_id: result.comp_id.unwrap(),
                };
                log_debug!(self.ctx, "Preempt result {}", result.spec.id);

                self.file_storage
                    .running_results
                    .borrow_mut()
                    .remove(&result.spec.id);
            }
            ResultState::Reading => {
                result.state = ResultState::Canceled;
                log_debug!(self.ctx, "Cancel result {}", result.spec.id);
            }
            _ => {
                panic!("Cannot preempt result with state {:?}", result.state);
            }
        }
    }

    fn on_work_fetch(&self) {
        if self.is_suspended() {
            return;
        }
        let sim_result = self.perform_rr_sim(false);

        let projects_ref = self.projects.borrow();
        if sim_result.work_fetch_req.estimated_delay < self.config.buffered_work_min {
            self.network.borrow_mut().send_event(
                sim_result.work_fetch_req,
                self.ctx.id(),
                *projects_ref.keys().next().unwrap(),
            );
        }

        self.ctx
            .emit_self(AskForWork {}, self.config.work_fetch_interval);
    }

    fn report_status(&self) {
        log_info!(
            self.ctx,
            "STATUS: {}",
            if self.is_suspended() {
                "SUSPENDED"
            } else {
                "ACTIVE"
            }
        );
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
        if self.is_active() {
            self.ctx
                .emit_self(ReportStatus {}, self.config.report_status_interval);
        }
    }
}

impl StaticEventHandler for Client {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
            StartClient { finish_time } => {
                self.on_start(finish_time);
            }
            Suspend {} => {
                if self.is_active() {
                    self.on_suspend();
                }
            }
            Resume {} => {
                if self.is_active() {
                    self.on_resume();
                }
            }
            WorkFetchReply { requests } => {
                if self.is_active() {
                    self.on_result_requests(requests, event.src);
                }
            }
            CompPreempted { .. } => {}
            CompContinued { .. } => {}
            ScheduleResults {} => {
                if self.is_active() {
                    self.schedule_results();
                }
            }
            AskForWork {} => {
                if self.is_active() {
                    self.on_work_fetch();
                }
            }
            ReportStatus {} => {
                if self.is_active() {
                    self.report_status();
                }
            }
            Finish {} => {
                *self.is_active.borrow_mut() = false;
                log_info!(
                    self.ctx,
                    "Simulation finished. No new events will be processed"
                );
            }
        })
    }
}
