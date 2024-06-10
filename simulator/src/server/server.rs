use memory_stats::memory_stats;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug, log_info};

use super::assimilator::Assimilator;
use super::data_server::DataServer;
use super::database::BoincDatabase;
use super::db_purger::DBPurger;
use super::feeder::Feeder;
use super::file_deleter::FileDeleter;
use super::job::*;
use super::job_generator::JobGenerator;
use super::scheduler::Scheduler;
use super::stats::ServerStats;
use super::transitioner::Transitioner;
use super::validator::Validator;
use crate::client::client::{ClientRegister, ResultCompleted, WorkFetchRequest};
use crate::common::Finish;
use crate::config::sim_config::ServerConfig;
use crate::server::data_server::InputFileDownloadCompleted;
use crate::server::database::ClientInfo;
use crate::simulator::simulator::StartServer;

pub const UNSENT_RESULT_BUFFER_LOWER_BOUND: usize = 1000;

#[derive(Clone, Serialize)]
pub struct ServerRegister {}

#[derive(Clone, Serialize)]
pub struct EnvokeTransitioner {}

#[derive(Clone, Serialize)]
pub struct EnvokeFeeder {}

#[derive(Clone, Serialize)]
pub struct AssimilateResults {}

#[derive(Clone, Serialize)]
pub struct AssimilationDone {
    pub workunit_id: WorkunitId,
}

#[derive(Clone, Serialize)]
pub struct ValidateResults {}

#[derive(Clone, Serialize)]
pub struct PurgeDB {}

#[derive(Clone, Serialize)]
pub struct DeleteFiles {}

#[derive(Clone, Serialize)]
pub struct GenerateJobs {
    pub cnt: usize,
}

#[derive(Clone, Serialize)]
pub struct JobsGenerationCompleted {}

#[derive(Clone, Serialize)]
pub struct SimulationProgress {
    progress: f64,
}

pub struct Server {
    // db
    pub db: Rc<BoincDatabase>,
    //job_generator
    job_generator: JobGenerator,
    //daemons
    pub validator: Validator,
    pub assimilator: Assimilator,
    pub transitioner: Transitioner,
    pub feeder: Feeder,
    pub file_deleter: FileDeleter,
    pub db_purger: DBPurger,
    // scheduler
    pub scheduler: Rc<RefCell<Scheduler>>,
    // data server
    pub data_server: Rc<RefCell<DataServer>>,
    //
    pub ctx: SimulationContext,
    config: ServerConfig,
    pub stats: Rc<RefCell<ServerStats>>,
    is_active: bool,
    pub rs_dur_sum: f64,
    pub check_dur: f64,
    pub memory: f64,
}

impl Server {
    pub fn new(
        database: Rc<BoincDatabase>,
        job_generator: JobGenerator,
        validator: Validator,
        assimilator: Assimilator,
        transitioner: Transitioner,
        feeder: Feeder,
        file_deleter: FileDeleter,
        db_purger: DBPurger,
        scheduler: Rc<RefCell<Scheduler>>,
        data_server: Rc<RefCell<DataServer>>,
        ctx: SimulationContext,
        config: ServerConfig,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        scheduler.borrow_mut().set_server_id(ctx.id());
        data_server.borrow_mut().set_server_id(ctx.id());
        Self {
            db: database,
            job_generator,
            validator,
            assimilator,
            transitioner,
            feeder,
            file_deleter,
            db_purger,
            scheduler,
            data_server,
            ctx,
            config,
            stats,
            is_active: true,
            rs_dur_sum: 0.,
            check_dur: 0.,
            memory: f64::MIN,
        }
    }

    pub fn on_started(&self, finish_time: f64) {
        log_info!(self.ctx, "started");
        self.ctx.emit_self_now(EnvokeTransitioner {});
        self.ctx
            .emit_self(ValidateResults {}, self.config.validator.interval);
        self.ctx
            .emit_self(AssimilateResults {}, self.config.assimilator.interval);
        self.ctx.emit_self_now(EnvokeFeeder {});
        self.ctx
            .emit_self(PurgeDB {}, self.config.db_purger.interval);
        self.ctx
            .emit_self(DeleteFiles {}, self.config.file_deleter.interval);
        self.ctx
            .emit(Finish {}, self.data_server.borrow().id, finish_time);
        for i in (0..100).step_by(1) {
            let progress = i as f64 / 100.;
            self.ctx
                .emit_self(SimulationProgress { progress }, progress * finish_time);
        }
        self.ctx.emit_self(Finish {}, finish_time);
    }

    fn on_client_register(&mut self, client_id: Id, speed: f64, cores: u32, memory: u64) {
        let client = ClientInfo {
            id: client_id,
            speed,
            cores,
            memory,
            credit: 0.,
        };
        log_debug!(self.ctx, "registered client {:?}", client);
        self.db.clients.borrow_mut().insert(client.id, client);
    }

    async fn on_job_spec(&self, specs: Vec<JobSpec>) {
        let mut wus = Vec::new();
        let mut input_files = Vec::new();

        for spec in specs {
            let mut workunit = WorkunitInfo {
                id: spec.id,
                spec,
                result_ids: Vec::new(),
                client_ids: Vec::new(),
                transition_time: f64::MAX,
                need_validate: false,
                file_delete_state: FileDeleteState::Init,
                canonical_resultid: None,
                assimilate_state: AssimilateState::Init,
            };
            workunit.spec.input_file.workunit_id = workunit.id;
            input_files.push(workunit.spec.input_file.clone());

            wus.push(workunit);
        }

        self.data_server
            .borrow()
            .download_input_files_from_server(input_files, wus[0].id);

        self.ctx
            .recv_event_by_key::<InputFileDownloadCompleted>(wus[0].id)
            .await;

        let mut db_result_mut = self.db.result.borrow_mut();

        for mut workunit in wus {
            for i in 0..workunit.spec.target_nresults {
                let result = ResultInfo {
                    id: *self.transitioner.next_result_id.borrow() + i,
                    workunit_id: workunit.id,
                    report_deadline: 0.,
                    server_state: ResultState::Unsent,
                    outcome: ResultOutcome::Undefined,
                    validate_state: ValidateState::Init,
                    file_delete_state: FileDeleteState::Init,
                    in_shared_mem: false,
                    time_sent: 0.,
                    client_id: 0,
                    is_correct: false,
                    claimed_credit: 0.,
                };
                self.db.feeder_result_ids.borrow_mut().push_back(result.id);
                workunit.result_ids.push(result.id);
                db_result_mut.insert(result.id, result);
            }
            *self.transitioner.next_result_id.borrow_mut() += workunit.spec.target_nresults;

            self.db.insert_new_workunit(workunit);
        }
    }

    fn on_result_completed(&mut self, result_id: ResultId, is_correct: bool, claimed_credit: f64) {
        if !self.db.result.borrow().contains_key(&result_id) {
            log_debug!(
                self.ctx,
                "received result {:?} too late, it is already deleted ",
                result_id
            );
            return;
        }

        log_info!(self.ctx, "completed result {:?}", result_id);

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        let result = db_result_mut.get_mut(&result_id).unwrap();
        let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();
        if result.outcome == ResultOutcome::Undefined {
            result.server_state = ResultState::Over;
            result.outcome = ResultOutcome::Success;
            result.validate_state = ValidateState::Init;
            result.claimed_credit = claimed_credit;
            result.is_correct = is_correct;
            self.db.update_wu_transition_time(workunit, self.ctx.time());
        }

        let processing_time = self.ctx.time() - result.time_sent;
        self.stats.borrow_mut().results_processing_time += processing_time;
        self.stats.borrow_mut().n_results_completed += 1;

        let min_processing_time = self.stats.borrow_mut().min_result_processing_time;
        self.stats.borrow_mut().min_result_processing_time =
            min_processing_time.min(processing_time);
        let max_processing_time = self.stats.borrow_mut().max_result_processing_time;
        self.stats.borrow_mut().max_result_processing_time =
            max_processing_time.max(processing_time);

        self.stats.borrow_mut().gflops_total += workunit.spec.gflops;
    }

    // ******* daemons **********

    fn envoke_feeder(&mut self, reschedule: bool) {
        let t = Instant::now();

        self.feeder.scan_work_array();

        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().feeder_sum_dur += duration;
        self.stats.borrow_mut().feeder_samples += 1;

        if reschedule {
            self.ctx
                .emit_self(EnvokeFeeder {}, self.config.feeder.interval);
        }

        if let Some(usage) = memory_stats() {
            self.memory = self.memory.max(usage.physical_mem as f64);
        } else {
            println!("Couldn't get the current memory usage :(");
        }
    }

    fn schedule_results(&mut self, client_id: Id, req: WorkFetchRequest) {
        if self.feeder.get_shared_memory_size() < UNSENT_RESULT_BUFFER_LOWER_BOUND
            && !self.db.feeder_result_ids.borrow().is_empty()
        {
            self.envoke_feeder(false);
        }

        let t = Instant::now();

        self.scheduler.borrow_mut().schedule(client_id, req);

        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().scheduler_sum_dur += duration;
        self.stats.borrow_mut().scheduler_samples += 1;

        if self.db.feeder_result_ids.borrow().len() + self.feeder.get_shared_memory_size()
            < UNSENT_RESULT_BUFFER_LOWER_BOUND
        {
            log_debug!(
                self.ctx,
                "Shared memory size: {}. Generated new workunits",
                self.feeder.get_shared_memory_size()
            );
            self.ctx.spawn(self.generate_jobs());
        }
    }

    fn envoke_transitioner(&mut self) {
        let t = Instant::now();

        self.transitioner.transit(self.ctx.time());

        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().transitioner_sum_dur += duration;
        self.stats.borrow_mut().transitioner_samples += 1;

        self.ctx
            .emit_self(EnvokeTransitioner {}, self.config.transitioner.interval);
    }

    fn validate_results(&mut self) {
        let t = Instant::now();

        self.validator.validate();

        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().validator_sum_dur += duration;
        self.stats.borrow_mut().validator_samples += 1;

        self.ctx
            .emit_self(ValidateResults {}, self.config.validator.interval);
    }

    async fn assimilate_results(&self) {
        let t = Instant::now();

        self.assimilator.assimilate().await;

        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().assimilator_sum_dur += duration;
        self.stats.borrow_mut().assimilator_samples += 1;

        self.ctx
            .emit_self(AssimilateResults {}, self.config.assimilator.interval);
    }

    fn delete_files(&mut self) {
        let t = Instant::now();

        self.file_deleter.delete_files();

        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().file_deleter_sum_dur += duration;
        self.stats.borrow_mut().file_deleter_samples += 1;

        self.ctx
            .emit_self(DeleteFiles {}, self.config.file_deleter.interval);
    }

    fn purge_db(&mut self) {
        let t = Instant::now();

        self.db_purger.purge_database();

        let duration = t.elapsed().as_secs_f64();
        self.stats.borrow_mut().db_purger_sum_dur += duration;
        self.stats.borrow_mut().db_purger_samples += 1;

        self.ctx
            .emit_self(PurgeDB {}, self.config.db_purger.interval);
    }

    pub async fn generate_jobs(&self) {
        let new_jobs = self.job_generator.generate_jobs(5000);
        self.on_job_spec(new_jobs).await;
    }

    pub async fn generate_jobs_init(&self, cnt: usize, simulation_id: Id) {
        let new_jobs: Vec<JobSpec> = self.job_generator.generate_jobs(cnt);
        self.on_job_spec(new_jobs).await;
        self.ctx.emit_now(JobsGenerationCompleted {}, simulation_id);
    }
}

impl EventHandler for Server {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            GenerateJobs { cnt } => {
                self.ctx.spawn(self.generate_jobs_init(cnt, event.src));
            }
            StartServer { finish_time } => {
                self.on_started(finish_time);
            }
            ClientRegister {
                speed,
                cores,
                memory,
            } => {
                self.on_client_register(event.src, speed, cores, memory);
            }
            ResultCompleted {
                result_id,
                is_correct,
                claimed_credit,
            } => {
                if self.is_active {
                    self.on_result_completed(result_id, is_correct, claimed_credit);
                }
            }
            WorkFetchRequest {
                req_secs,
                req_instances,
                estimated_delay,
            } => {
                if self.is_active {
                    self.schedule_results(
                        event.src,
                        WorkFetchRequest {
                            req_secs,
                            req_instances,
                            estimated_delay,
                        },
                    );
                }
            }
            ValidateResults {} => {
                if self.is_active {
                    self.validate_results();
                }
            }
            AssimilateResults {} => {
                if self.is_active {
                    self.ctx.spawn(self.assimilate_results());
                }
            }
            EnvokeTransitioner {} => {
                if self.is_active {
                    self.envoke_transitioner();
                }
            }
            EnvokeFeeder {} => {
                if self.is_active {
                    self.envoke_feeder(true);
                }
            }
            PurgeDB {} => {
                if self.is_active {
                    self.purge_db();
                }
            }
            DeleteFiles {} => {
                if self.is_active {
                    self.delete_files();
                }
            }
            Finish {} => {
                self.is_active = false;
                log_info!(
                    self.ctx,
                    "Simulation finished. No new events will be processed"
                );
            }
            SimulationProgress { progress } => {
                println!("Simulation progress: {:.0}%", progress * 100.);
            }
        })
    }
}
