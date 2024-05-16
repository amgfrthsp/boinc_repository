use dslab_compute::multicore::Compute;
use dslab_core::context::SimulationContext;
use dslab_core::{log_info, Id};
use std::cell::RefCell;
use std::rc::Rc;

use crate::client::client::{ContinueResult, ExecuteResult};
use crate::client::storage::FileStorage;
use crate::client::task::ResultState;
use crate::server::job::ResultId;

use super::rr_simulation::RRSimulation;
use super::utils::Utilities;

pub struct Scheduler {
    client_id: Id,
    rr_sim: Rc<RefCell<RRSimulation>>,
    compute: Rc<RefCell<Compute>>,
    file_storage: Rc<FileStorage>,
    utilities: Rc<RefCell<Utilities>>,
    ctx: SimulationContext,
}

impl Scheduler {
    pub fn new(
        rr_sim: Rc<RefCell<RRSimulation>>,
        compute: Rc<RefCell<Compute>>,
        file_storage: Rc<FileStorage>,
        utilities: Rc<RefCell<Utilities>>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            client_id: 0,
            rr_sim,
            compute,
            file_storage,
            utilities,
            ctx,
        }
    }

    pub fn set_client_id(&mut self, client_id: Id) {
        self.client_id = client_id;
    }

    pub fn schedule(&self) -> bool {
        log_info!(self.ctx, "scheduling started");

        let sim_result = self.rr_sim.borrow_mut().simulate(true);

        let results_to_schedule = sim_result.results_to_schedule;
        let n_results_to_schedule = results_to_schedule.len();

        log_info!(
            self.ctx,
            "All results: {}; ready to schedule: {}; running: {}",
            self.file_storage.results.borrow().len(),
            results_to_schedule.len(),
            self.file_storage.running_results.borrow().len()
        );

        let mut cores_available = self.compute.borrow().cores_total();
        let mut memory_available = self.compute.borrow().memory_total();

        let mut scheduled_results: Vec<ResultId> = Vec::new();

        let mut fs_results = self.file_storage.results.borrow_mut();

        let mut skip = 0;
        let mut cont = 0;
        let mut start = 0;
        let mut preempt = 0;

        for result_id in results_to_schedule {
            let result = fs_results.get_mut(&result_id).unwrap();
            if result.spec.cores > cores_available || result.spec.memory > memory_available {
                //log_info!(self.ctx, "Skip result {}", result_id);
                skip += 1;
                continue;
            }
            cores_available -= result.spec.cores;
            memory_available -= result.spec.memory;

            if let ResultState::Preempted { comp_id } = result.state {
                self.ctx
                    .emit_now(ContinueResult { result_id, comp_id }, self.client_id);

                //log_info!(self.ctx, "Continue result {}", result_id);
                cont += 1;
            } else if result.state == ResultState::Unstarted {
                self.ctx
                    .emit_now(ExecuteResult { result_id }, self.client_id);

                //log_info!(self.ctx, "Start result {}", result_id);
                start += 1;
            } else {
                // log_info!(
                //     self.ctx,
                //     "Keep result {} with state {:?} running",
                //     result_id,
                //     result.state
                // );
            }
            scheduled_results.push(result_id);
        }

        let clone = self.file_storage.running_results.borrow().clone();

        for result_id in clone {
            let result = fs_results.get_mut(&result_id).unwrap();
            if !scheduled_results.contains(&result_id)
                && !(result.state == ResultState::Running
                    && self.utilities.borrow().is_running_finished(result))
            {
                self.utilities.borrow().preempt_result(result);
                preempt += 1;
            }
        }

        log_info!(
            self.ctx,
            "scheduling finished. skip {} continue {} start {} preempt {}",
            skip,
            cont,
            start,
            preempt
        );

        n_results_to_schedule - scheduled_results.len() > 0
    }
}
