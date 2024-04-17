use dslab_compute::multicore::Compute;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, Id};
use std::cell::RefCell;
use std::rc::Rc;

use crate::client::client::{ContinueResult, ExecuteResult};
use crate::client::storage::FileStorage;
use crate::client::task::ResultState;
use crate::server::job::ResultId;

use super::task::ResultInfo;

pub struct Scheduler {
    client_id: Id,
    compute: Rc<RefCell<Compute>>,
    file_storage: Rc<FileStorage>,
    ctx: SimulationContext,
}

impl Scheduler {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        file_storage: Rc<FileStorage>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            client_id: 0,
            compute,
            file_storage,
            ctx,
        }
    }

    pub fn set_client_id(&mut self, client_id: Id) {
        self.client_id = client_id;
    }

    pub fn schedule(&self) -> bool {
        log_info!(self.ctx, "scheduling started");

        let results_to_schedule =
            FileStorage::get_map_keys_by_predicate(&self.file_storage.results.borrow(), |result| {
                result.state == ResultState::ReadyToExecute
                    || result.state == ResultState::Running
                    || matches!(result.state, ResultState::Preempted { .. })
            });

        let n_results_to_schedule = results_to_schedule.len();

        log_debug!(
            self.ctx,
            "Found {} results with state [ReadyToExecute, Running, Preempted]",
            n_results_to_schedule
        );

        let mut fs_results = self.file_storage.results.borrow_mut();

        let mut deadline_missed: Vec<(ResultId, f64)> = Vec::new();
        let mut results_queue: Vec<(ResultId, f64)> = Vec::new();

        for result_id in results_to_schedule {
            let result = fs_results.get(&result_id).unwrap();
            log_debug!(self.ctx, "Results_to schedule: {:?}", result);
            if self.deadline_missed(result) {
                deadline_missed.push((result_id, result.report_deadline));
            } else {
                results_queue.push((result_id, result.report_deadline));
            }
        }

        log_debug!(
            self.ctx,
            "Performed simulation: results {:?} will likely miss their deadline",
            deadline_missed
        );

        log_debug!(
            self.ctx,
            "Performed simulation: results {:?} are in queue",
            results_queue
        );

        deadline_missed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let mut cores_available = self.compute.borrow().cores_total();
        let mut memory_available = self.compute.borrow().memory_total();

        let mut scheduled_results: Vec<ResultId> = Vec::new();

        for (result_id, _) in [deadline_missed, results_queue].concat() {
            let result = fs_results.get_mut(&result_id).unwrap();
            log_debug!(
                self.ctx,
                "Result {} {:?}: {} {} {} {}",
                result_id,
                result.state,
                result.spec.min_cores,
                cores_available,
                result.spec.memory,
                memory_available,
            );
            if result.spec.min_cores > cores_available || result.spec.memory > memory_available {
                continue;
            }
            let cores = cores_available.min(result.spec.max_cores);
            cores_available -= cores;
            memory_available -= result.spec.memory;

            if let ResultState::Preempted { comp_id } = result.state {
                self.ctx
                    .emit_now(ContinueResult { result_id, comp_id }, self.client_id);

                log_debug!(self.ctx, "Continue result {}", result_id);
            } else if result.state == ResultState::ReadyToExecute {
                self.ctx
                    .emit_now(ExecuteResult { result_id }, self.client_id);

                log_debug!(self.ctx, "Start result {}", result_id);
            }
            scheduled_results.push(result_id);
        }

        for (result_id, comp_id) in self.file_storage.running_results.borrow().clone() {
            if !scheduled_results.contains(&result_id) {
                self.compute.borrow_mut().preempt_computation(comp_id);

                let result = fs_results.get_mut(&result_id).unwrap();
                result.state = ResultState::Preempted { comp_id };

                log_debug!(self.ctx, "Preempt result {}", result_id);

                self.file_storage
                    .running_results
                    .borrow_mut()
                    .remove(&(result_id, comp_id));
            }
        }

        log_info!(self.ctx, "scheduling finished");

        n_results_to_schedule - scheduled_results.len() > 0
    }

    pub fn deadline_missed(&self, result: &ResultInfo) -> bool {
        self.ctx.time() + self.est_result_runtime(result, self.compute.borrow().cores_total())
            >= result.report_deadline
    }

    /// Returns estimated runtime for the remaining part of the result
    pub fn est_result_runtime(&self, result: &ResultInfo, cores: u32) -> f64 {
        let fraction_done;

        if result.state == ResultState::ReadyToExecute {
            // Unstarted
            fraction_done = 0.;
        } else if result.state == ResultState::Running
            || matches!(result.state, ResultState::Preempted { .. })
        {
            fraction_done = self.compute.borrow().fraction_done(result.comp_id.unwrap());
        } else {
            panic!("Cannot estimate runtime for a task");
        }

        (1. - fraction_done)
            * self.compute.borrow().est_compute_time(
                result.spec.flops,
                cores,
                result.spec.cores_dependency,
            )
    }
}
