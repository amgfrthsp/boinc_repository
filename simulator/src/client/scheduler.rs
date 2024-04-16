use dslab_compute::multicore::Compute;
use dslab_core::context::SimulationContext;
use dslab_core::{log_info, Id};
use std::cell::RefCell;
use std::rc::Rc;

use crate::client::client::ExecuteResult;
use crate::client::storage::FileStorage;
use crate::client::task::ResultState;

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

    pub fn schedule(&self, _cores_available: u32, _memory_available: u64) -> bool {
        log_info!(self.ctx, "scheduling started");

        let results_to_schedule =
            FileStorage::get_map_keys_by_predicate(&self.file_storage.results.borrow(), |result| {
                result.state == ResultState::ReadyToExecute
            });

        let batch_size = 3;

        for result_id in results_to_schedule.iter().take(batch_size) {
            self.ctx.emit_now(
                ExecuteResult {
                    result_id: *result_id,
                },
                self.client_id,
            );
        }

        log_info!(self.ctx, "scheduling finished");

        results_to_schedule.len() > batch_size
    }

    /// Returns estimated runtime for the remaining part of the result
    pub fn est_result_runtime(&self, result: &ResultInfo, cores: u32) -> f64 {
        if result.state == ResultState::ReadyToExecute {
            // Unstarted result
            self.compute.borrow().est_compute_time(
                result.spec.flops,
                cores,
                result.spec.cores_dependency,
            )
        } else if result.state == ResultState::Running || result.state == ResultState::Preempted {
            // Already running or preempted result
            self.compute.borrow().fraction_done(result.comp_id.unwrap())
                * self.compute.borrow().est_compute_time(
                    result.spec.flops,
                    cores,
                    result.spec.cores_dependency,
                )
        } else {
            panic!("Cannot estimate runtime for a task");
        }
    }
}
