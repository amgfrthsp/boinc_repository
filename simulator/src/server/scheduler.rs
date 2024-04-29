use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, Event, EventHandler};
use dslab_network::Network;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use crate::client::rr_simulation::WorkFetchRequest;
use crate::config::sim_config::SchedulerConfig;
use crate::server::job::{OutputFileMetadata, ResultRequest, ResultState};

use super::database::BoincDatabase;
use super::job::JobSpec;
use super::server::ClientInfo;

pub struct Scheduler {
    server_id: Id,
    net: Rc<RefCell<Network>>,
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: SchedulerConfig,
}

impl Scheduler {
    pub fn new(
        net: Rc<RefCell<Network>>,
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            server_id: 0,
            net,
            db,
            ctx,
            config,
        }
    }

    pub fn set_server_id(&mut self, server_id: Id) {
        self.server_id = server_id;
    }

    pub fn schedule(&mut self, client_info: &ClientInfo, mut req: WorkFetchRequest) {
        let results_to_schedule =
            BoincDatabase::get_map_keys_by_predicate(&self.db.result.borrow(), |result| {
                result.in_shared_mem
            });

        log_info!(self.ctx, "scheduling started");
        log_debug!(self.ctx, "results to schedule: {:?}", results_to_schedule);

        let t = Instant::now();
        let mut assigned_results = Vec::new();
        let mut assigned_results_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        for result_id in results_to_schedule {
            let result = db_result_mut.get_mut(&result_id).unwrap();
            let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();

            let est_runtime = self.get_est_runtime(&workunit.spec);

            if req.estimated_delay + est_runtime < workunit.spec.delay_bound {
                log_debug!(
                    self.ctx,
                    "assigned result {} to client {}",
                    result_id,
                    client_info.id
                );

                req.estimated_delay += est_runtime;
                req.req_secs -= est_runtime;
                req.req_instances -= workunit.spec.cores;

                result.server_state = ResultState::InProgress;
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
            }
        }
        self.net
            .borrow_mut()
            .send_event(assigned_results, self.server_id, client_info.id);

        let schedule_duration = t.elapsed();
        log_info!(
            self.ctx,
            "scheduling finished: assigned {} results in {:.2?}",
            assigned_results_cnt,
            schedule_duration
        );
    }

    pub fn get_est_runtime(&self, spec: &JobSpec) -> f64 {
        0.
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, _event: Event) {}
}
