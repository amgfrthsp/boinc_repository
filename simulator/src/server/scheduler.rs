use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, Event, EventHandler};
use dslab_network::Network;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use crate::client::client::{WorkFetchReply, WorkFetchRequest};
use crate::config::sim_config::SchedulerConfig;
use crate::server::feeder::SharedMemoryItemState;
use crate::server::job::{OutputFileMetadata, ResultRequest, ResultState};

use super::database::BoincDatabase;
use super::feeder::SharedMemoryItem;
use super::job::JobSpec;
use super::server::ClientInfo;

pub struct Scheduler {
    server_id: Id,
    net: Rc<RefCell<Network>>,
    db: Rc<BoincDatabase>,
    shared_memory: Rc<RefCell<Vec<SharedMemoryItem>>>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: SchedulerConfig,
}

impl Scheduler {
    pub fn new(
        net: Rc<RefCell<Network>>,
        db: Rc<BoincDatabase>,
        shared_memory: Rc<RefCell<Vec<SharedMemoryItem>>>,
        ctx: SimulationContext,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            server_id: 0,
            net,
            db,
            shared_memory,
            ctx,
            config,
        }
    }

    pub fn set_server_id(&mut self, server_id: Id) {
        self.server_id = server_id;
    }

    pub fn schedule(&mut self, client_info: &ClientInfo, mut req: WorkFetchRequest) {
        log_info!(self.ctx, "scheduling started");

        let t = Instant::now();
        let mut assigned_results = Vec::new();
        let mut assigned_results_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();
        let mut shmem_mut = self.shared_memory.borrow_mut();

        let shmem_size = shmem_mut.len();
        let start_ind = self.ctx.gen_range(0..shmem_size);
        let mut i = (start_ind + 1) % shmem_size;

        log_debug!(self.ctx, "Starting from index {}", i);

        while i != start_ind && !(req.req_secs < 0. && req.req_instances < 0) {
            let item = shmem_mut.get_mut(i).unwrap();
            i += 1;
            i %= shmem_size;
            if item.state == SharedMemoryItemState::Empty {
                continue;
            }
            if !db_result_mut.contains_key(&item.result_id) {
                item.state = SharedMemoryItemState::Empty;
                continue;
            }

            let result = db_result_mut.get_mut(&item.result_id).unwrap();
            let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();

            if result.server_state != ResultState::Unsent {
                item.state = SharedMemoryItemState::Empty;
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

                item.state = SharedMemoryItemState::Empty;

                req.estimated_delay += est_runtime;
                req.req_secs -= est_runtime;
                req.req_instances -= workunit.spec.cores as i32;

                result.in_shared_mem = false;
                result.server_state = ResultState::InProgress;
                result.time_sent = self.ctx.time();
                result.report_deadline = self.ctx.time() + workunit.spec.delay_bound;
                workunit.transition_time =
                    f64::min(workunit.transition_time, result.report_deadline);

                assigned_results_cnt += 1;

                let mut spec = workunit.spec.clone();
                spec.id = result.id;
                assigned_results.push(ResultRequest {
                    spec,
                    report_deadline: result.report_deadline,
                    output_file: OutputFileMetadata {
                        result_id: result.id,
                        size: self.ctx.gen_range(10..=100),
                    },
                });
            } else {
                log_debug!(self.ctx, "Skipping result {}", result.id);
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
            "scheduling finished: assigned {} results in {:.2?} for client {}",
            assigned_results_cnt,
            schedule_duration,
            client_info.id
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
