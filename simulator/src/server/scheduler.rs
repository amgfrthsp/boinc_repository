use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, Event, EventHandler};
use dslab_network::Network;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Instant;

use crate::client::client::{WorkFetchReply, WorkFetchRequest};
use crate::server::job::{ResultRequest, ResultState};

use super::database::BoincDatabase;
use super::job::{JobSpec, ResultId};
use super::stats::ServerStats;

pub struct Scheduler {
    server_id: Id,
    net: Rc<RefCell<Network>>,
    db: Rc<BoincDatabase>,
    shared_memory: Rc<RefCell<VecDeque<ResultId>>>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    stats: Rc<RefCell<ServerStats>>,
    pub dur_sum: f64,
    dur_samples: usize,
}

impl Scheduler {
    pub fn new(
        net: Rc<RefCell<Network>>,
        db: Rc<BoincDatabase>,
        shared_memory: Rc<RefCell<VecDeque<ResultId>>>,
        ctx: SimulationContext,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        Self {
            server_id: 0,
            net,
            db,
            shared_memory,
            ctx,
            stats,
            dur_samples: 0,
            dur_sum: 0.,
        }
    }

    pub fn set_server_id(&mut self, server_id: Id) {
        self.server_id = server_id;
    }

    pub fn schedule(&mut self, client_id: Id, mut req: WorkFetchRequest) {
        log_info!(
            self.ctx,
            "scheduling started. shared memory size is {}",
            self.shared_memory.borrow().len()
        );
        log_debug!(self.ctx, "work fetch request is {:?}", req);

        let clients_ref = self.db.clients.borrow();
        let client_info = clients_ref.get(&client_id).unwrap();

        let t = Instant::now();
        let mut assigned_results = Vec::new();
        let mut assigned_results_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        let mut shmem = self.shared_memory.borrow_mut();

        while !shmem.is_empty() && !(req.req_secs < 0. && req.req_instances < 0) {
            let result_id = shmem.pop_front().unwrap();
            if !db_result_mut.contains_key(&result_id) {
                continue;
            }
            let result = db_result_mut.get_mut(&result_id).unwrap();
            let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();

            if result.server_state != ResultState::Unsent || !result.in_shared_mem {
                result.in_shared_mem = false;
                continue;
            }

            let est_runtime = self.get_est_runtime(&workunit.spec, client_info.speed);

            // !workunit.client_ids.contains(&client_info.id) &&
            if workunit.spec.cores <= client_info.cores
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
                let new_wu_t_time = f64::min(workunit.transition_time, result.report_deadline);
                self.db.update_wu_transition_time(workunit, new_wu_t_time);
                workunit.client_ids.push(client_info.id);

                assigned_results_cnt += 1;

                let mut spec = workunit.spec.clone();
                spec.id = result.id;
                spec.output_file.result_id = result.id;
                assigned_results.push(ResultRequest {
                    spec,
                    report_deadline: result.report_deadline,
                });
            } else {
                shmem.push_back(result_id);
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

        let schedule_duration = t.elapsed().as_secs_f64();
        self.dur_sum += schedule_duration;
        self.dur_samples += 1;
        log_info!(
            self.ctx,
            "scheduling finished: assigned {} results in {:.2?} for client {}.shared memory size is {}",
            assigned_results_cnt,
            schedule_duration,
            client_info.id,
            shmem.len()
        );
    }

    // FIX: take into account I/O
    pub fn get_est_runtime(&self, spec: &JobSpec, client_speed: f64) -> f64 {
        spec.gflops / client_speed / spec.cores_dependency.speedup(spec.cores)
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, _event: Event) {}
}
