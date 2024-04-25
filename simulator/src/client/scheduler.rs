use dslab_compute::multicore::Compute;
use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, Id};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use crate::client::client::{ContinueResult, ExecuteResult};
use crate::client::storage::FileStorage;
use crate::client::task::ResultState;
use crate::server::job::ResultId;

use super::task::ResultInfo;

pub struct SchedulerSimulationResult {
    results_to_schedule: Vec<ResultId>,
}

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

        let sim_result = self.simulate();

        let mut results_to_schedule = sim_result.results_to_schedule;
        let n_results_to_schedule = results_to_schedule.len();

        results_to_schedule.sort_by(|a, b| self.result_cmp(a, b));

        let mut cores_available = self.compute.borrow().cores_total();
        let mut memory_available = self.compute.borrow().memory_total();

        let mut scheduled_results: Vec<ResultId> = Vec::new();

        let mut fs_results = self.file_storage.results.borrow_mut();

        for result_id in results_to_schedule {
            let result = fs_results.get_mut(&result_id).unwrap();
            if result.spec.cores > cores_available || result.spec.memory > memory_available {
                log_debug!(self.ctx, "Skip result {}", result_id);
                continue;
            }
            cores_available -= result.spec.cores;
            memory_available -= result.spec.memory;

            if let ResultState::Preempted { comp_id } = result.state {
                self.ctx
                    .emit_now(ContinueResult { result_id, comp_id }, self.client_id);

                log_debug!(self.ctx, "Continue result {}", result_id);
            } else if result.state == ResultState::Unstarted {
                self.ctx
                    .emit_now(ExecuteResult { result_id }, self.client_id);

                log_debug!(self.ctx, "Start result {}", result_id);
            } else {
                log_debug!(
                    self.ctx,
                    "Keep result {} with state {:?} running",
                    result_id,
                    result.state
                );
            }
            scheduled_results.push(result_id);
        }

        let clone = self.file_storage.running_results.borrow().clone();

        for result_id in clone {
            let result = fs_results.get_mut(&result_id).unwrap();
            if !scheduled_results.contains(&result_id)
                && !(result.state == ResultState::Running && self.is_running_finished(result))
            {
                self.preempt_result(result);
            }
        }

        log_info!(self.ctx, "scheduling finished");

        n_results_to_schedule - scheduled_results.len() > 0
    }

    pub fn result_cmp(&self, result1_id: &ResultId, result2_id: &ResultId) -> Ordering {
        let fs_results = self.file_storage.results.borrow();

        let result1 = fs_results.get(result1_id).unwrap();
        let result2 = fs_results.get(result2_id).unwrap();

        if result1.sim_miss_deadline && !result2.sim_miss_deadline {
            return Ordering::Less;
        }
        if !result1.sim_miss_deadline && result2.sim_miss_deadline {
            return Ordering::Greater;
        }
        if result1.spec.cores != result2.spec.cores {
            return result1.spec.cores.cmp(&result2.spec.cores);
        }
        if result1.sim_miss_deadline {
            return result1
                .report_deadline
                .partial_cmp(&result2.report_deadline)
                .unwrap();
        } else {
            match result1.state {
                ResultState::Reading => {
                    if result2.state == ResultState::Reading {
                        return Ordering::Less;
                    } else if let ResultState::Preempted { .. } = result2.state {
                        return Ordering::Less;
                    } else if result2.state == ResultState::Unstarted {
                        return Ordering::Less;
                    }
                }
                ResultState::Running => {
                    if let ResultState::Preempted { .. } = result2.state {
                        return Ordering::Less;
                    } else if result2.state == ResultState::Unstarted {
                        return Ordering::Less;
                    }
                }
                ResultState::Preempted { .. } => {
                    if result2.state == ResultState::Running {
                        return Ordering::Greater;
                    } else if result2.state == ResultState::Unstarted {
                        return Ordering::Less;
                    }
                }
                ResultState::Unstarted => {
                    if result2.state != ResultState::Unstarted {
                        return Ordering::Greater;
                    }
                }
                _ => {
                    panic!("Invalid result state");
                }
            }
            return result1.time_added.partial_cmp(&result2.time_added).unwrap();
        }
    }

    pub fn simulate(&self) -> SchedulerSimulationResult {
        let results_to_consider =
            FileStorage::get_map_keys_by_predicate(&self.file_storage.results.borrow(), |result| {
                result.state == ResultState::Unstarted
                    || result.state == ResultState::Reading
                    || result.state == ResultState::Running
                    || matches!(result.state, ResultState::Preempted { .. })
            });

        log_debug!(
            self.ctx,
            "Found {} results with state [Unstarted, Reading, Running, Preempted]",
            results_to_consider.len()
        );

        let mut fs_results = self.file_storage.results.borrow_mut();
        let mut results_to_schedule = Vec::new();

        for result_id in &results_to_consider {
            let result = fs_results.get_mut(result_id).unwrap();
            result.sim_miss_deadline = false;
            if result.state == ResultState::Running && self.is_running_finished(result) {
                continue;
            }
            log_debug!(self.ctx, "Result to schedule: {:?}", result);
            if self.deadline_missed(result) {
                result.sim_miss_deadline = true;
            }
            results_to_schedule.push(*result_id);
        }

        SchedulerSimulationResult {
            results_to_schedule,
        }
    }

    // Might be the case when in compute computation is finished and comp_id is deleted
    // But Scheduling started earlier than CompFinished came to client and result's state got updated
    pub fn is_running_finished(&self, result: &ResultInfo) -> bool {
        self.compute
            .borrow()
            .fraction_done(result.comp_id.unwrap())
            .is_err()
    }

    pub fn deadline_missed(&self, result: &ResultInfo) -> bool {
        self.ctx.time() + self.est_result_runtime(result, self.compute.borrow().cores_total())
            >= result.report_deadline
    }

    /// Returns estimated runtime for the remaining part of the result
    /// FIX: Take reading/writing/uploading time into account
    pub fn est_result_runtime(&self, result: &ResultInfo, cores: u32) -> f64 {
        let fraction_done;

        if result.state == ResultState::Unstarted {
            fraction_done = 0.;
        } else if result.state == ResultState::Reading {
            fraction_done = 0.;
        } else if result.state == ResultState::Running
            || matches!(result.state, ResultState::Preempted { .. })
        {
            // is_running_finished()
            fraction_done = self
                .compute
                .borrow()
                .fraction_done(result.comp_id.unwrap())
                .unwrap();
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

    pub fn preempt_result(&self, result: &mut ResultInfo) {
        match result.state {
            ResultState::Running => {
                self.compute
                    .borrow_mut()
                    .preempt_computation(result.comp_id.unwrap());

                result.state = ResultState::Preempted {
                    comp_id: result.comp_id.unwrap(),
                };
                log_debug!(self.ctx, "Preempt result {}", result.spec.id);

                self.file_storage
                    .running_results
                    .borrow_mut()
                    .remove(&result.spec.id);
            }
            ResultState::Reading => {
                result.state = ResultState::Unstarted;
                log_debug!(self.ctx, "Cancel result {}", result.spec.id);
            }
            _ => {
                panic!("Cannot preempt result with state {:?}", result.state);
            }
        }
    }
}
