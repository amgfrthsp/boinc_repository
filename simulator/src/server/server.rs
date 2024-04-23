use log::log_enabled;
use log::Level::Info;
use priority_queue::PriorityQueue;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
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
use crate::client::client::{ClientRegister, ResultCompleted, ResultsInquiry};
use crate::common::{ReportStatus, Start};
use crate::config::sim_config::ServerConfig;
use crate::server::data_server::InputFileDownloadCompleted;

#[derive(Clone, Serialize)]
pub struct ServerRegister {}

#[derive(Clone, Serialize)]
pub struct ScheduleJobs {}

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
    #[allow(dead_code)]
    state: ClientState,
    speed: f64,
    cpus_total: u32,
    pub cpus_available: u32,
    memory_total: u64,
    pub memory_available: u64,
}

pub type ClientScore = (u64, u32, u64);

impl ClientInfo {
    pub fn score(&self) -> ClientScore {
        (
            self.memory_available,
            self.cpus_available,
            (self.speed * 1000.) as u64,
        )
    }
}

pub struct Server {
    clients: BTreeMap<Id, ClientInfo>,
    client_queue: PriorityQueue<Id, ClientScore>,
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
    pub scheduling_time: f64,
    scheduling_planned: RefCell<bool>,
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
        data_server.borrow_mut().set_server_id(ctx.id());
        Self {
            clients: BTreeMap::new(),
            client_queue: PriorityQueue::new(),
            db: database,
            validator,
            assimilator,
            transitioner,
            feeder,
            file_deleter,
            db_purger,
            scheduler,
            data_server,
            scheduling_time: 0.,
            scheduling_planned: RefCell::new(false),
            ctx,
            config,
        }
    }

    fn on_started(&mut self) {
        log_debug!(self.ctx, "started");
        *self.scheduling_planned.borrow_mut() = true;
        self.ctx.emit_self(ScheduleJobs {}, 1.);
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

    fn on_client_register(
        &mut self,
        client_id: Id,
        cpus_total: u32,
        memory_total: u64,
        speed: f64,
    ) {
        let client = ClientInfo {
            id: client_id,
            state: ClientState::Online,
            speed,
            cpus_total,
            cpus_available: cpus_total,
            memory_total,
            memory_available: memory_total,
        };
        log_debug!(self.ctx, "registered client {:?}", client);
        self.client_queue.push(client_id, client.score());
        self.clients.insert(client.id, client);
    }

    async fn on_job_spec(&self, mut spec: JobSpec, from: Id) {
        log_debug!(self.ctx, "job spec {:?}", spec.clone());

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

        let planned = *self.scheduling_planned.borrow();
        if !planned {
            *self.scheduling_planned.borrow_mut() = true;
            self.ctx.emit_self(ScheduleJobs {}, 10.);
        }
    }

    fn on_work_inquiry(&mut self, client_id: Id) {
        log_info!(self.ctx, "client {} asks for work", client_id);
        let client = self.clients.get(&client_id).unwrap();
        self.client_queue.push(client_id, client.score());
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

        let client = self.clients.get_mut(&client_id).unwrap();
        client.cpus_available += workunit.spec.cores;
        client.memory_available += workunit.spec.memory;
    }

    // ******* daemons **********

    fn envoke_feeder(&mut self) {
        self.feeder.scan_work_array();
        if self.is_active() {
            self.ctx
                .emit_self(EnvokeFeeder {}, self.config.feeder.interval);
        }
    }

    fn schedule_results(&mut self) {
        self.scheduler
            .borrow_mut()
            .schedule(&mut self.clients, &mut self.client_queue);

        *self.scheduling_planned.borrow_mut() = false;
        if self.is_active() {
            *self.scheduling_planned.borrow_mut() = true;
            self.ctx
                .emit_self(ScheduleJobs {}, self.config.scheduler.interval);
        }
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
        !BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
            wu.canonical_resultid.is_none()
        })
        .is_empty()
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
            Start {} => {
                self.on_started();
            }
            ScheduleJobs {} => {
                self.schedule_results();
            }
            ClientRegister {
                speed,
                cpus_total,
                memory_total,
            } => {
                self.on_client_register(event.src, cpus_total, memory_total, speed);
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
                    },
                    event.src,
                ));
            }
            ResultCompleted { result_id } => {
                self.on_result_completed(result_id, event.src);
            }
            ReportStatus {} => {
                self.report_status();
            }
            ResultsInquiry {} => {
                self.on_work_inquiry(event.src)
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
        })
    }
}
