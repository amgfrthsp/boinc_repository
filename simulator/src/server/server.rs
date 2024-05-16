use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;

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
use crate::common::{Finish, ReportStatus};
use crate::config::sim_config::ServerConfig;
use crate::server::data_server::InputFileDownloadCompleted;
use crate::server::database::ClientInfo;
use crate::simulator::simulator::StartServer;

const UNSENT_RESULT_BUFFER_LOWER_BOUND: usize = 200;

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
pub struct CheckWorkunitBuffer {}

#[derive(Clone, Serialize)]
pub struct GenerateJobs {
    pub cnt: usize,
}

#[derive(Clone, Serialize)]
pub struct JobsGenerationCompleted {}

pub struct Server {
    // db
    pub db: Rc<BoincDatabase>,
    //job_generator
    job_generator: JobGenerator,
    //daemons
    validator: Validator,
    assimilator: Assimilator,
    transitioner: Transitioner,
    feeder: Feeder,
    file_deleter: FileDeleter,
    db_purger: DBPurger,
    // scheduler
    scheduler: Rc<RefCell<Scheduler>>,
    // data server
    pub data_server: Rc<RefCell<DataServer>>,
    //
    pub ctx: SimulationContext,
    config: ServerConfig,
    pub stats: Rc<RefCell<ServerStats>>,
    is_active: bool,
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
            .emit_self(ReportStatus {}, self.config.report_status_interval);
        self.ctx.emit(
            ReportStatus {},
            self.data_server.borrow().id,
            self.config.report_status_interval,
        );
        self.ctx.emit_self(CheckWorkunitBuffer {}, 1500.);
        self.ctx
            .emit(Finish {}, self.data_server.borrow().id, finish_time);
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

    async fn on_job_spec(&self, mut spec: JobSpec) {
        // log_debug!(self.ctx, "job spec {:?}", spec.clone());

        let workunit = WorkunitInfo {
            id: spec.id,
            spec: spec.clone(),
            result_ids: Vec::new(),
            client_ids: Vec::new(),
            transition_time: self.ctx.time(),
            need_validate: false,
            file_delete_state: FileDeleteState::Init,
            canonical_resultid: None,
            assimilate_state: AssimilateState::Init,
        };

        spec.input_file.workunit_id = workunit.id;

        // log_debug!(
        //     self.ctx,
        //     "input file download started for workunit {}",
        //     workunit.id
        // );

        self.data_server
            .borrow_mut()
            .download_file(DataServerFile::Input(spec.input_file), self.ctx.id());

        self.ctx
            .recv_event_by_key::<InputFileDownloadCompleted>(workunit.id)
            .await;

        // log_debug!(
        //     self.ctx,
        //     "input file download finished for workunit {}",
        //     workunit.id
        // );

        self.db.insert_new_workunit(workunit);
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

    fn envoke_feeder(&mut self) {
        self.feeder.scan_work_array();
        self.ctx
            .emit_self(EnvokeFeeder {}, self.config.feeder.interval);
    }

    fn schedule_results(&mut self, client_id: Id, req: WorkFetchRequest) {
        self.scheduler.borrow_mut().schedule(client_id, req);
    }

    fn envoke_transitioner(&mut self) {
        self.transitioner.transit(self.ctx.time());
        self.ctx
            .emit_self(EnvokeTransitioner {}, self.config.transitioner.interval);
    }

    fn validate_results(&mut self) {
        self.validator.validate();
        self.ctx
            .emit_self(ValidateResults {}, self.config.validator.interval);
    }

    fn assimilate_results(&mut self) {
        self.assimilator.assimilate();
        self.ctx
            .emit_self(AssimilateResults {}, self.config.assimilator.interval);
    }

    fn delete_files(&mut self) {
        self.file_deleter.delete_files();
        self.ctx
            .emit_self(DeleteFiles {}, self.config.file_deleter.interval);
    }

    fn purge_db(&mut self) {
        self.db_purger.purge_database();
        self.ctx
            .emit_self(PurgeDB {}, self.config.db_purger.interval);
    }

    pub fn generate_jobs(&mut self) {
        let new_jobs = self
            .job_generator
            .generate_jobs(self.db.clients.borrow().len());
        for job in new_jobs {
            self.ctx.spawn(self.on_job_spec(job));
        }
    }

    pub async fn generate_jobs_and_wait(&self, cnt: usize, simulation_id: Id) {
        let new_jobs = self.job_generator.generate_jobs(cnt);
        futures::future::join_all(new_jobs.into_iter().map(|j| self.on_job_spec(j))).await;
        self.ctx.emit_now(JobsGenerationCompleted {}, simulation_id);
    }

    pub fn check_wu_buffer(&mut self) {
        if self.feeder.get_shared_memory_size() < UNSENT_RESULT_BUFFER_LOWER_BOUND {
            log_debug!(
                self.ctx,
                "Shared memory size: {}. Generated new workunits",
                self.feeder.get_shared_memory_size()
            );
            self.generate_jobs();
        }
        self.ctx.emit_self(CheckWorkunitBuffer {}, 1500.);
    }

    // ******* utilities & statistics *********
    fn report_status(&mut self) {
        log_info!(
            self.ctx,
            "UNSENT RESULTS: {} / IN PROGRESS RESULTS: {} / COMPLETED RESULTS: {}",
            BoincDatabase::get_map_keys_by_predicate(&self.db.result.borrow(), |result| {
                result.server_state == ResultState::Unsent
            })
            .len(),
            BoincDatabase::get_map_keys_by_predicate(&self.db.result.borrow(), |result| {
                result.server_state == ResultState::InProgress
            })
            .len(),
            BoincDatabase::get_map_keys_by_predicate(&self.db.result.borrow(), |result| {
                result.server_state == ResultState::Over
            })
            .len()
        );
        self.ctx
            .emit_self(ReportStatus {}, self.config.report_status_interval);
        self.ctx.emit(
            ReportStatus {},
            self.data_server.borrow().id,
            self.config.report_status_interval,
        );
    }
}

impl EventHandler for Server {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            GenerateJobs { cnt } => {
                self.ctx.spawn(self.generate_jobs_and_wait(cnt, event.src));
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
            ReportStatus {} => {
                if self.is_active {
                    self.report_status();
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
                    self.assimilate_results();
                }
            }
            EnvokeTransitioner {} => {
                if self.is_active {
                    self.envoke_transitioner();
                }
            }
            EnvokeFeeder {} => {
                if self.is_active {
                    self.envoke_feeder();
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
            CheckWorkunitBuffer {} => {
                if self.is_active {
                    self.check_wu_buffer();
                }
            }
        })
    }
}
