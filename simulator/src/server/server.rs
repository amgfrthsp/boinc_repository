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
use dslab_network::Network;

use super::assimilator::Assimilator;
use super::data_server::DataServer;
use super::database::BoincDatabase;
use super::job::*;
use super::scheduler::Scheduler;
use super::transitioner::Transitioner;
use super::validator::Validator;
use crate::client::client::{ClientRegister, TaskCompleted, TasksInquiry};
use crate::common::Start;

#[derive(Clone, Serialize)]
pub struct ServerRegister {}

#[derive(Clone, Serialize)]
pub struct ReportStatus {}

#[derive(Clone, Serialize)]
pub struct ScheduleJobs {}

#[derive(Clone, Serialize)]
pub struct EnvokeTransitioner {}

#[derive(Clone, Serialize)]
pub struct AssimilateResults {}

#[derive(Clone, Serialize)]
pub struct AssimilationDone {
    pub(crate) workunit_id: u64,
}

#[derive(Clone, Serialize)]
pub struct ValidateResults {}

#[derive(Clone, Serialize)]
pub struct PurgeDB {}

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
    #[allow(dead_code)]
    cpus_total: u32,
    pub cpus_available: u32,
    #[allow(dead_code)]
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
    id: Id,
    net: Rc<RefCell<Network>>,
    clients: BTreeMap<Id, ClientInfo>,
    client_queue: PriorityQueue<Id, ClientScore>,
    // db
    db: Rc<BoincDatabase>,
    //daemons
    validator: Rc<RefCell<Validator>>,
    assimilator: Rc<RefCell<Assimilator>>,
    transitioner: Rc<RefCell<Transitioner>>,
    // scheduler
    scheduler: Rc<RefCell<Scheduler>>,
    // data server
    data_server: Rc<RefCell<DataServer>>,
    //
    cpus_total: u32,
    cpus_available: u32,
    memory_total: u64,
    memory_available: u64,
    pub scheduling_time: f64,
    scheduling_planned: bool,
    pub ctx: SimulationContext,
}

impl Server {
    pub fn new(
        net: Rc<RefCell<Network>>,
        database: Rc<BoincDatabase>,
        validator: Rc<RefCell<Validator>>,
        assimilator: Rc<RefCell<Assimilator>>,
        transitioner: Rc<RefCell<Transitioner>>,
        scheduler: Rc<RefCell<Scheduler>>,
        data_server: Rc<RefCell<DataServer>>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id(),
            net,
            clients: BTreeMap::new(),
            client_queue: PriorityQueue::new(),
            db: database,
            validator,
            assimilator,
            transitioner,
            scheduler,
            data_server,
            cpus_total: 0,
            cpus_available: 0,
            memory_total: 0,
            memory_available: 0,
            scheduling_time: 0.,
            scheduling_planned: false,
            ctx,
        }
    }

    fn on_started(&mut self) {
        log_debug!(self.ctx, "started");
        self.scheduling_planned = true;
        self.ctx.emit_self(ScheduleJobs {}, 1.);
        self.ctx.emit_self(EnvokeTransitioner {}, 3.);
        self.ctx.emit_self(ValidateResults {}, 50.);
        self.ctx.emit_self(AssimilateResults {}, 20.);
        self.ctx.emit_self(PurgeDB {}, 60.);
        if log_enabled!(Info) {
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
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
        log_debug!(self.ctx, "registered client: {:?}", client);
        self.cpus_total += client.cpus_total;
        self.cpus_available += client.cpus_available;
        self.memory_total += client.memory_total;
        self.memory_available += client.memory_available;
        self.client_queue.push(client_id, client.score());
        self.clients.insert(client.id, client);
    }

    fn on_job_request(&mut self, req: JobRequest, from: Id) {
        log_debug!(self.ctx, "job request: {:?}", req.clone());

        let workunit = WorkunitInfo {
            id: req.id,
            req: req.clone(),
            result_ids: Vec::new(),
            transition_time: self.ctx.time(),
            delay_bound: 250.,
            min_quorum: 2,
            target_nresults: 3,
            need_validate: false,
            file_delete_state: FileDeleteState::Init,
            canonical_resultid: None,
            assimilate_state: AssimilateState::Init,
        };

        self.db.workunit.borrow_mut().insert(workunit.id, workunit);

        self.data_server
            .borrow_mut()
            .load_input_file_for_workunit(req.input_file, from);

        if !self.scheduling_planned {
            self.scheduling_planned = true;
            self.ctx.emit_self(ScheduleJobs {}, 10.);
        }
    }

    fn on_jobs_inquiry(&mut self, client_id: Id) {
        log_info!(self.ctx, "client {} asks for work", client_id);
        let client = self.clients.get(&client_id).unwrap();
        self.client_queue.push(client_id, client.score());
    }

    fn on_result_completed(&mut self, result_id: u64, client_id: Id) {
        log_debug!(self.ctx, "completed result: {:?}", result_id);

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        let result = db_result_mut.get_mut(&result_id).unwrap();
        let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();
        if result.outcome.is_none() {
            result.server_state = ResultState::Over;
            result.outcome = Some(ResultOutcome::Success);
            result.validate_state = Some(ValidateState::Init);
            workunit.transition_time = self.ctx.time();
        }

        let client = self.clients.get_mut(&client_id).unwrap();
        client.cpus_available += workunit.req.min_cores;
        client.memory_available += workunit.req.memory;
        self.cpus_available += workunit.req.min_cores;
        self.memory_available += workunit.req.memory;
    }

    // ******* daemons **********

    fn schedule_results(&mut self) {
        self.scheduler.borrow_mut().schedule(
            &mut self.cpus_available,
            &mut self.memory_available,
            &mut self.clients,
            &mut self.client_queue,
            self.ctx.time(),
        );

        self.scheduling_planned = false;
        if self.is_active() {
            self.scheduling_planned = true;
            self.ctx.emit_self(ScheduleJobs {}, 10.);
        }
    }

    fn envoke_transitioner(&mut self) {
        self.transitioner.borrow().transit(self.ctx.time());
        if self.is_active() {
            self.ctx.emit_self(EnvokeTransitioner {}, 3.);
        }
    }

    fn validate_results(&mut self) {
        self.validator.borrow().validate();
        if self.is_active() {
            self.ctx.emit_self(ValidateResults {}, 50.);
        }
    }

    fn assimilate_results(&mut self) {
        self.assimilator.borrow().assimilate();
        if self.is_active() {
            self.ctx.emit_self(AssimilateResults {}, 20.);
        }
    }

    fn purge_db(&mut self) {}

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
            "CPU: {:.2} / MEMORY: {:.2} / UNSENT RESULTS: {} / IN PROGRESS RESULTS: {} / COMPLETED RESULTS: {}",
            (self.cpus_total - self.cpus_available) as f64 / self.cpus_total as f64,
            (self.memory_total - self.memory_available) as f64 / self.memory_total as f64,
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
            self.ctx.emit_self(ReportStatus {}, 100.);
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
            JobRequest {
                id,
                flops,
                memory,
                min_cores,
                max_cores,
                cores_dependency,
                input_file,
                output_file,
            } => {
                self.on_job_request(
                    JobRequest {
                        id,
                        flops,
                        memory,
                        min_cores,
                        max_cores,
                        cores_dependency,
                        input_file,
                        output_file,
                    },
                    event.src,
                );
            }
            TaskCompleted { id } => {
                self.on_result_completed(id, event.src);
            }
            ReportStatus {} => {
                self.report_status();
            }
            TasksInquiry {} => {
                self.on_jobs_inquiry(event.src)
            }
            ValidateResults {} => {
                self.validate_results();
            }
            PurgeDB {} => {
                self.purge_db();
            }
            AssimilateResults {} => {
                self.assimilate_results();
            }
            EnvokeTransitioner {} => {
                self.envoke_transitioner();
            }
        })
    }
}
