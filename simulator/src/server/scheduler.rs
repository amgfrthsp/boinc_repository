use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, Event, EventHandler};
use dslab_network::Network;
use priority_queue::PriorityQueue;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

use crate::config::sim_config::SchedulerConfig;
use crate::server::database::DBResultState;
use crate::server::job::{OutputFileMetadata, ResultRequest, ResultState};

use super::database::BoincDatabase;
use super::server::{ClientInfo, ClientScore};

// TODO:
// 1. Clock skew?

pub struct Scheduler {
    id: Id,
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
            id: ctx.id(),
            net,
            db,
            ctx,
            config,
        }
    }

    pub fn schedule(
        &mut self,
        clients: &mut BTreeMap<Id, ClientInfo>,
        clients_queue: &mut PriorityQueue<Id, ClientScore>,
    ) {
        let results_to_schedule = self.db.get_results_with_state(DBResultState::ServerState {
            state: ResultState::Unsent,
        });

        log_info!(self.ctx, "scheduling started");
        log_debug!(self.ctx, "results to schedule: {:?}", results_to_schedule);

        let t = Instant::now();
        let mut assigned_results_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();

        for result_id in results_to_schedule {
            if clients_queue.is_empty() {
                break;
            }

            let mut db_result_mut = self.db.result.borrow_mut();
            let result = db_result_mut.get_mut(&result_id).unwrap();
            let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();

            let mut checked_clients = Vec::new();

            while let Some((client_id, (memory, cpus, speed))) = clients_queue.pop() {
                if cpus >= workunit.spec.cores && memory >= workunit.spec.memory {
                    log_debug!(
                        self.ctx,
                        "assigned result {} to client {}",
                        result_id,
                        client_id
                    );

                    // update state
                    result.report_deadline = self.ctx.time() + workunit.spec.delay_bound;
                    workunit.transition_time =
                        f64::min(workunit.transition_time, result.report_deadline);

                    assigned_results_cnt += 1;

                    // update client record
                    let client = clients.get_mut(&client_id).unwrap();
                    client.cpus_available -= workunit.spec.cores;
                    client.memory_available -= workunit.spec.memory;
                    checked_clients.push((client.id, client.score()));

                    // send result instance to client
                    let mut spec = workunit.spec.clone();
                    spec.id = result.id;
                    let request = ResultRequest {
                        spec,
                        report_deadline: result.report_deadline,
                        output_file: OutputFileMetadata {
                            result_id: result.id,
                            size: self.ctx.gen_range(10..=100),
                        },
                    };
                    self.net
                        .borrow_mut()
                        .send_event(request, self.id, client_id);

                    // already borrowed problem
                    self.db.change_result_state(
                        result,
                        DBResultState::ServerState {
                            state: ResultState::InProgress,
                        },
                    );

                    break;
                } else {
                    checked_clients.push((client_id, (memory, cpus, speed)));
                }
                if memory <= workunit.spec.memory {
                    break;
                }
            }
            for (client_id, (memory, cpus, speed)) in checked_clients.into_iter() {
                if memory > 0 && cpus > 0 {
                    clients_queue.push(client_id, (memory, cpus, speed));
                }
            }
        }
        let schedule_duration = t.elapsed();
        log_info!(
            self.ctx,
            "scheduling finished: assigned {} results in {:.2?}",
            assigned_results_cnt,
            schedule_duration
        );
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, event: Event) {}
}
