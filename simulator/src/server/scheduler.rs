use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, Event, EventHandler};
use dslab_network::Network;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use crate::client::client::{WorkFetchReply, WorkFetchRequest};
use crate::config::sim_config::SchedulerConfig;
use crate::server::job::{ResultRequest, ResultState};

use super::database::{BoincDatabase, ClientInfo};
use super::job::{JobSpec, ResultId};
use super::stats::ServerStats;

pub struct Scheduler {
    server_id: Id,
    net: Rc<RefCell<Network>>,
    db: Rc<BoincDatabase>,
    shared_memory: Rc<RefCell<Vec<ResultId>>>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: SchedulerConfig,
    stats: Rc<RefCell<ServerStats>>,
}

impl Scheduler {
    pub fn new(
        net: Rc<RefCell<Network>>,
        db: Rc<BoincDatabase>,
        shared_memory: Rc<RefCell<Vec<ResultId>>>,
        ctx: SimulationContext,
        config: SchedulerConfig,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        Self {
            server_id: 0,
            net,
            db,
            shared_memory,
            ctx,
            config,
            stats,
        }
    }

    pub fn set_server_id(&mut self, server_id: Id) {
        self.server_id = server_id;
    }

    pub fn schedule(&mut self, client_info: &ClientInfo, mut req: WorkFetchRequest) {
        log_info!(
            self.ctx,
            "scheduling started. shared memory size is {}",
            self.shared_memory.borrow().len()
        );

        let t = Instant::now();
        let mut assigned_results = Vec::new();
        let mut assigned_results_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        let shmem_clone = self.shared_memory.borrow_mut().clone();
        self.shared_memory.borrow_mut().clear();

        for result_id in shmem_clone {
            if req.req_secs < 0. && req.req_instances < 0 {
                self.shared_memory.borrow_mut().push(result_id);
                continue;
            }
            if !db_result_mut.contains_key(&result_id) {
                continue;
            }
            let result = db_result_mut.get_mut(&result_id).unwrap();
            let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();

            if result.server_state != ResultState::Unsent {
                result.in_shared_mem = false;
                continue;
            }

            log_debug!(
                self.ctx,
                "Req secs {}; req_instances {}; estimated delay {}",
                req.req_secs,
                req.req_instances,
                req.estimated_delay
            );

            let est_runtime = self.get_est_runtime(&workunit.spec, client_info.speed);

            if !workunit.client_ids.contains(&client_info.id)
                && workunit.spec.cores <= client_info.cores
                && workunit.spec.memory <= client_info.memory
                && req.estimated_delay + est_runtime < workunit.spec.delay_bound
            {
                log_debug!(
                    self.ctx,
                    "assigned result {} to client {}",
                    result.id,
                    client_info.id
                );

                req.estimated_delay += est_runtime;
                req.req_secs -= est_runtime;
                req.req_instances -= workunit.spec.cores as i32;

                result.in_shared_mem = false;
                result.server_state = ResultState::InProgress;
                result.time_sent = self.ctx.time();
                result.client_id = client_info.id;
                result.report_deadline = self.ctx.time() + workunit.spec.delay_bound;
                workunit.transition_time =
                    f64::min(workunit.transition_time, result.report_deadline);
                workunit.client_ids.push(client_info.id);

                assigned_results_cnt += 1;
                self.stats.borrow_mut().n_results_total += 1;

                let mut spec = workunit.spec.clone();
                spec.id = result.id;
                spec.output_file.result_id = result.id;
                assigned_results.push(ResultRequest {
                    spec,
                    report_deadline: result.report_deadline,
                });
            } else {
                self.shared_memory.borrow_mut().push(result_id);
            }
        }

        if !assigned_results.is_empty() {
            self.net.borrow_mut().send_event(
                WorkFetchReply {
                    requests: assigned_results,
                },
                self.server_id,
                client_info.id,
            );
        }

        let schedule_duration = t.elapsed();
        log_info!(
            self.ctx,
            "scheduling finished: assigned {} results in {:.2?} for client {}.shared memory size is {}",
            assigned_results_cnt,
            schedule_duration,
            client_info.id,
            self.shared_memory.borrow().len()
        );
    }

    // FIX: take into account I/O
    pub fn get_est_runtime(&self, spec: &JobSpec, client_speed: f64) -> f64 {
        spec.flops / client_speed / spec.cores_dependency.speedup(spec.cores)
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, _event: Event) {}
}
