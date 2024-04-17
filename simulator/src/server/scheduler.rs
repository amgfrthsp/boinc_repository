use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, Event, EventHandler};
use dslab_network::Network;
use priority_queue::PriorityQueue;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

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
}

impl Scheduler {
    pub fn new(net: Rc<RefCell<Network>>, db: Rc<BoincDatabase>, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            net,
            db,
            ctx,
        }
    }

    pub fn schedule(
        &mut self,
        cpus_available: &mut u32,
        memory_available: &mut u64,
        clients: &mut BTreeMap<Id, ClientInfo>,
        clients_queue: &mut PriorityQueue<Id, ClientScore>,
    ) {
        let results_to_schedule =
            BoincDatabase::get_map_keys_by_predicate(&self.db.result.borrow(), |result| {
                result.server_state == ResultState::Unsent
            });

        log_info!(self.ctx, "scheduling started");

        let t = Instant::now();
        let mut assigned_results_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        for result_id in results_to_schedule {
            if clients_queue.is_empty() {
                break;
            }

            let result = db_result_mut.get_mut(&result_id).unwrap();
            let workunit = db_workunit_mut.get_mut(&result.workunit_id).unwrap();

            if workunit.spec.min_cores > *cpus_available || workunit.spec.memory > *memory_available
            {
                continue;
            }

            let mut checked_clients = Vec::new();

            while let Some((client_id, (memory, cpus, speed))) = clients_queue.pop() {
                if cpus >= workunit.spec.min_cores && memory >= workunit.spec.memory {
                    log_debug!(
                        self.ctx,
                        "assigned result {} to client {}",
                        result_id,
                        client_id
                    );

                    // update state
                    result.server_state = ResultState::InProgress;
                    result.report_deadline = self.ctx.time() + workunit.delay_bound;
                    workunit.transition_time =
                        f64::min(workunit.transition_time, result.report_deadline);

                    assigned_results_cnt += 1;

                    // update client record
                    let client = clients.get_mut(&client_id).unwrap();
                    client.cpus_available -= workunit.spec.min_cores;
                    client.memory_available -= workunit.spec.memory;
                    *cpus_available -= workunit.spec.min_cores;
                    *memory_available -= workunit.spec.memory;
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
