use dslab_core::async_mode::EventKey;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::HashMap;
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
use super::scheduler::Scheduler;
use super::transitioner::Transitioner;
use super::validator::Validator;
use crate::client::client::{ClientRegister, ResultCompleted, WorkFetchRequest};
use crate::common::ReportStatus;
use crate::config::sim_config::ServerConfig;
use crate::server::data_server::InputFileDownloadCompleted;
use crate::server::job_generator::AllJobsSent;
use crate::simulator::simulator::StartServer;

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

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum ClientState {
    Online,
    Offline,
}

#[derive(Debug)]
pub struct ClientInfo {
    pub id: Id,
    pub speed: f64,
    pub cores: u32,
    pub memory: u64,
}

pub struct Server {
    clients: HashMap<Id, ClientInfo>,
    // db
    db: Rc<BoincDatabase>,
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
    data_server: Rc<RefCell<DataServer>>,
    //
    received_all_jobs: bool,
    pub ctx: SimulationContext,
    config: ServerConfig,
}

impl Server {
    pub fn new(
        database: Rc<BoincDatabase>,
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
    ) -> Self {
        scheduler.borrow_mut().set_server_id(ctx.id());
        data_server.borrow_mut().set_server_id(ctx.id());
        Self {
            clients: HashMap::new(),
            db: database,
            validator,
            assimilator,
            transitioner,
            feeder,
            file_deleter,
            db_purger,
            scheduler,
            data_server,
            received_all_jobs: false,
            ctx,
            config,
        }
    }

    fn on_started(&mut self) {
        log_debug!(self.ctx, "started");
        self.ctx.emit_self(EnvokeTransitioner {}, 3.);
        self.ctx
            .emit_self(ValidateResults {}, self.config.validator.interval);
        self.ctx
            .emit_self(AssimilateResults {}, self.config.assimilator.interval);
        self.ctx
            .emit_self(EnvokeFeeder {}, self.config.feeder.interval);
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
    }

    fn on_client_register(&mut self, client_id: Id, speed: f64, cores: u32, memory: u64) {
        let client = ClientInfo {
            id: client_id,
            speed,
            cores,
            memory,
        };
        log_debug!(self.ctx, "registered client {:?}", client);
        self.clients.insert(client.id, client);
    }

    async fn on_job_spec(&self, mut spec: JobSpec, from: Id, event_id: EventKey) {
        log_debug!(self.ctx, "job spec {:?}", spec.clone());

        spec.event_id = event_id;

        let workunit = WorkunitInfo {
            id: spec.id,
            spec: spec.clone(),
            result_ids: Vec::new(),
            transition_time: self.ctx.time(),
            need_validate: false,
            file_delete_state: FileDeleteState::Init,
            canonical_resultid: None,
            assimilate_state: AssimilateState::Init,
        };

        spec.input_file.workunit_id = workunit.id;

        log_debug!(
            self.ctx,
            "input file download started for workunit {}",
            workunit.id
        );

        self.data_server
            .borrow_mut()
            .download_file(DataServerFile::Input(spec.input_file), from);

        self.ctx
            .recv_event_by_key::<InputFileDownloadCompleted>(workunit.id)
            .await;

        log_debug!(
            self.ctx,
            "input file download finished for workunit {}",
            workunit.id
        );

        self.db.workunit.borrow_mut().insert(workunit.id, workunit);
    }

    fn on_result_completed(&mut self, result_id: ResultId, client_id: Id) {
        if !self.db.result.borrow().contains_key(&result_id) {
            log_debug!(
                self.ctx,
                "received result {:?} too late, it is already deleted ",
                result_id
            );
            return;
        }

        log_debug!(self.ctx, "completed result {:?}", result_id);

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        let result = db_result_mut.get_mut(&result_id).unwrap();
        let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();
        if result.outcome == ResultOutcome::Undefined {
            result.server_state = ResultState::Over;
            result.outcome = ResultOutcome::Success;
            result.validate_state = ValidateState::Init;
            workunit.transition_time = self.ctx.time();
        }
    }

    // ******* daemons **********

    fn envoke_feeder(&mut self) {
        self.feeder.scan_work_array();
        if self.is_active() {
            self.ctx
                .emit_self(EnvokeFeeder {}, self.config.feeder.interval);
        }
    }

    fn schedule_results(&mut self, client_id: Id, req: WorkFetchRequest) {
        let client_info = self.clients.get(&client_id).unwrap();
        self.scheduler.borrow_mut().schedule(client_info, req);
    }

    fn envoke_transitioner(&mut self) {
        self.transitioner.transit(self.ctx.time());
        if self.is_active() {
            self.ctx
                .emit_self(EnvokeTransitioner {}, self.config.transitioner.interval);
        }
    }

    fn validate_results(&mut self) {
        self.validator.validate();
        if self.is_active() {
            self.ctx
                .emit_self(ValidateResults {}, self.config.validator.interval);
        }
    }

    fn assimilate_results(&mut self) {
        self.assimilator.assimilate();
        if self.is_active() {
            self.ctx
                .emit_self(AssimilateResults {}, self.config.assimilator.interval);
        }
    }

    fn delete_files(&mut self) {
        self.file_deleter.delete_files();
        if self.is_active() {
            self.ctx
                .emit_self(DeleteFiles {}, self.config.file_deleter.interval);
        }
    }

    fn purge_db(&mut self) {
        self.db_purger.purge_database();
        if self.is_active() {
            self.ctx
                .emit_self(PurgeDB {}, self.config.db_purger.interval);
        }
    }

    // ******* utilities & statistics *********

    fn is_active(&self) -> bool {
        let is_active =
            !BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.canonical_resultid.is_none()
            })
            .is_empty();

        if !is_active {
            for (client_id, _) in &self.clients {
                self.ctx.emit_now(AllJobsSent {}, *client_id);
            }
        }
        is_active
    }

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
        if self.is_active() {
            self.ctx
                .emit_self(ReportStatus {}, self.config.report_status_interval);
            self.ctx.emit(
                ReportStatus {},
                self.data_server.borrow().id,
                self.config.report_status_interval,
            );
        }
    }
}

impl EventHandler for Server {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            StartServer {} => {
                self.on_started();
            }
            ClientRegister {
                speed,
                cores,
                memory,
            } => {
                self.on_client_register(event.src, speed, cores, memory);
            }
            JobSpec {
                id,
                flops,
                memory,
                cores,
                cores_dependency,
                delay_bound,
                min_quorum,
                target_nresults,
                input_file,
                event_id,
            } => {
                self.ctx.spawn(self.on_job_spec(
                    JobSpec {
                        id,
                        flops,
                        memory,
                        cores,
                        cores_dependency,
                        delay_bound,
                        min_quorum,
                        target_nresults,
                        input_file,
                        event_id,
                    },
                    event.src,
                    event.id,
                ));
            }
            ResultCompleted { result_id } => {
                self.on_result_completed(result_id, event.src);
            }
            ReportStatus {} => {
                self.report_status();
            }
            WorkFetchRequest {
                req_secs,
                req_instances,
                estimated_delay,
            } => {
                self.schedule_results(
                    event.src,
                    WorkFetchRequest {
                        req_secs,
                        req_instances,
                        estimated_delay,
                    },
                );
            }
            ValidateResults {} => {
                self.validate_results();
            }
            AssimilateResults {} => {
                self.assimilate_results();
            }
            EnvokeTransitioner {} => {
                self.envoke_transitioner();
            }
            EnvokeFeeder {} => {
                self.envoke_feeder();
            }
            PurgeDB {} => {
                self.purge_db();
            }
            DeleteFiles {} => {
                self.delete_files();
            }
            AllJobsSent {} => {
                self.received_all_jobs = true;
            }
        })
    }
}
