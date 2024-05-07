use dslab_compute::multicore::Compute;
use dslab_core::context::SimulationContext;
use dslab_core::log_debug;
use serde::Serialize;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::rc::Rc;

use crate::client::storage::FileStorage;
use crate::client::task::ResultState;
use crate::common::FloatWrapper;
use crate::server::job::ResultId;

use super::client::WorkFetchRequest;
use super::task::ResultInfo;
use super::utils::Utilities;

#[derive(Clone, Serialize)]
pub struct RRSimulationResult {
    pub results_to_schedule: Vec<ResultId>,
    pub work_fetch_req: WorkFetchRequest,
}

pub struct RRSimulation {
    buffered_work_lower_bound: f64,
    buffered_work_upper_bound: f64,
    file_storage: Rc<FileStorage>,
    compute: Rc<RefCell<Compute>>,
    utilities: Rc<RefCell<Utilities>>,
    ctx: SimulationContext,
}

impl RRSimulation {
    pub fn new(
        buffered_work_lower_bound: f64,
        buffered_work_upper_bound: f64,
        file_storage: Rc<FileStorage>,
        compute: Rc<RefCell<Compute>>,
        utilities: Rc<RefCell<Utilities>>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            buffered_work_lower_bound,
            buffered_work_upper_bound,
            file_storage,
            compute,
            utilities,
            ctx,
        }
    }

    pub fn simulate(&self, is_scheduling: bool) -> RRSimulationResult {
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
        let mut results_to_schedule: Vec<&ResultInfo> = Vec::new();

        for result_id in &results_to_consider {
            let result = fs_results.get_mut(result_id).unwrap();
            result.sim_miss_deadline = false;
            if result.state == ResultState::Running
                && self.utilities.borrow().is_running_finished(result)
            {
                continue;
            }
            if is_scheduling {
                log_debug!(self.ctx, "Result to schedule: {:?}", result);
            }
            if self.deadline_missed(result) {
                result.sim_miss_deadline = true;
            }
        }
        for result_id in &results_to_consider {
            let result = fs_results.get(result_id).unwrap();
            if result.state == ResultState::Running
                && self.utilities.borrow().is_running_finished(result)
            {
                continue;
            }
            results_to_schedule.push(result);
        }

        results_to_schedule.sort_by(|a, b| self.result_cmp(a, b));

        let mut cores_release_time = BinaryHeap::<FloatWrapper>::new();
        for _ in 0..self.compute.borrow().cores_total() {
            cores_release_time.push(FloatWrapper(0.));
        }

        for result in &results_to_schedule {
            let est_runtime = self.est_result_runtime(*result);
            // We do not simulate fractional number of cores
            let mut start_time: f64 = 0.;
            for _ in 0..result.spec.cores {
                start_time = start_time.max(cores_release_time.pop().unwrap().0);
            }
            let finish_time = FloatWrapper(start_time + est_runtime);
            for _ in 0..result.spec.cores {
                cores_release_time.push(finish_time.clone());
            }
        }

        let mut shortfall = 0.;
        let mut busy_time = cores_release_time.peek().unwrap().0;

        for _ in 0..self.compute.borrow().cores_total() {
            let core_released = cores_release_time.pop().unwrap().0;
            busy_time = busy_time.min(core_released);
            if core_released < self.buffered_work_lower_bound {
                shortfall += (self.buffered_work_upper_bound - core_released).max(0.);
            }
        }

        let work_fetch_req = WorkFetchRequest {
            req_secs: shortfall,
            req_instances: self.compute.borrow().cores_available() as i32,
            estimated_delay: busy_time,
        };

        if !is_scheduling {
            log_debug!(self.ctx, "WorkFetchRequest: {:?}", work_fetch_req);
        }

        RRSimulationResult {
            results_to_schedule: results_to_schedule
                .into_iter()
                .map(|r| r.spec.id)
                .collect::<Vec<_>>(),
            work_fetch_req,
        }
    }

    pub fn deadline_missed(&self, result: &ResultInfo) -> bool {
        self.ctx.time() + self.est_result_runtime(result) >= result.report_deadline
    }

    /// Returns estimated runtime for the remaining part of the result
    /// FIX: Take reading/writing/uploading time into account
    pub fn est_result_runtime(&self, result: &ResultInfo) -> f64 {
        let fraction_done;

        if result.state == ResultState::Unstarted {
            fraction_done = 0.;
        } else if result.state == ResultState::Reading {
            fraction_done = 0.;
        } else if result.state == ResultState::Running
            || matches!(result.state, ResultState::Preempted { .. })
        {
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
                result.spec.cores,
                result.spec.cores_dependency,
            )
    }

    pub fn result_cmp(&self, result1: &ResultInfo, result2: &ResultInfo) -> Ordering {
        if result1.sim_miss_deadline && !result2.sim_miss_deadline {
            return Ordering::Less;
        }
        if !result1.sim_miss_deadline && result2.sim_miss_deadline {
            return Ordering::Greater;
        }
        if result1.spec.cores != result2.spec.cores {
            return result1.spec.cores.cmp(&result2.spec.cores).reverse();
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
}
