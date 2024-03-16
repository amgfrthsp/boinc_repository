use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, log_trace, Event, EventHandler};
use dslab_network::Network;
use priority_queue::PriorityQueue;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::Instant;

use crate::server::job::ResultState;

use super::job::{ResultInfo, WorkunitInfo};
use super::server::{ClientInfo, ClientScore};

// TODO:
// 1. Clock skew?

pub struct Scheduler {
    id: Id,
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl Scheduler {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        return Self {
            id: ctx.id(),
            net,
            ctx,
        };
    }

    pub fn schedule(
        &self,
        results_to_schedule: Vec<u64>,
        workunits_db: &mut HashMap<u64, Rc<RefCell<WorkunitInfo>>>,
        results_db: &mut HashMap<u64, Rc<RefCell<ResultInfo>>>,
        cpus_available: &mut u32,
        memory_available: &mut u64,
        clients: &mut BTreeMap<Id, ClientInfo>,
        clients_queue: &mut PriorityQueue<Id, ClientScore>,
        current_time: f64,
    ) {
        log_trace!(self.ctx, "scheduling {} results", results_to_schedule.len());
        let t = Instant::now();
        let mut assigned_results_cnt = 0;
        for result_id in results_to_schedule {
            if clients_queue.is_empty() {
                break;
            }
            let mut result = results_db.get_mut(&result_id).unwrap().borrow_mut();
            let mut workunit = workunits_db
                .get_mut(&result.workunit_id)
                .unwrap()
                .borrow_mut();
            if workunit.req.min_cores > *cpus_available || workunit.req.memory > *memory_available {
                continue;
            }
            let mut checked_clients = Vec::new();
            while let Some((client_id, (memory, cpus, speed))) = clients_queue.pop() {
                if cpus >= workunit.req.min_cores && memory >= workunit.req.memory {
                    log_debug!(
                        self.ctx,
                        "assigned result {} to client {}",
                        result_id,
                        client_id
                    );
                    result.server_state = ResultState::InProgress;
                    result.report_deadline = current_time + workunit.delay_bound;
                    workunit.transition_time =
                        f64::min(workunit.transition_time, result.report_deadline);
                    assigned_results_cnt += 1;
                    let client = clients.get_mut(&client_id).unwrap();
                    client.cpus_available -= workunit.req.min_cores;
                    client.memory_available -= workunit.req.memory;
                    *cpus_available -= workunit.req.min_cores;
                    *memory_available -= workunit.req.memory;
                    checked_clients.push((client.id, client.score()));
                    let mut result_wu_req = workunit.req.clone();
                    result_wu_req.id = result.id;
                    self.net
                        .borrow_mut()
                        .send_event(result_wu_req, self.id, client_id);
                    break;
                } else {
                    checked_clients.push((client_id, (memory, cpus, speed)));
                }
                if memory <= workunit.req.memory {
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
            "schedule_results: assigned {} results in {:.2?}",
            assigned_results_cnt,
            schedule_duration
        );
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, event: Event) {}
}
