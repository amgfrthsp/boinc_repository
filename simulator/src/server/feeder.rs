use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use crate::config::sim_config::FeederConfig;
use crate::server::job::ResultState;

use super::database::BoincDatabase;
use super::job::ResultId;

#[derive(Serialize, Debug, Clone, PartialEq)]
pub enum SharedMemoryItemState {
    Empty,
    Present,
}

#[derive(Serialize, Debug, Clone)]
pub struct SharedMemoryItem {
    pub state: SharedMemoryItemState,
    pub result_id: ResultId,
}

pub struct Feeder {
    shared_memory: Rc<RefCell<Vec<ResultId>>>,
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: FeederConfig,
    pub dur_sum: f64,
    dur_samples: usize,
}

impl Feeder {
    pub fn new(
        shared_memory: Rc<RefCell<Vec<ResultId>>>,
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        config: FeederConfig,
    ) -> Self {
        Self {
            shared_memory,
            db,
            ctx,
            config,
            dur_samples: 0,
            dur_sum: 0.,
        }
    }

    pub fn get_shared_memory_size(&self) -> usize {
        self.shared_memory.borrow().len()
    }

    pub fn scan_work_array(&mut self) {
        let t = Instant::now();
        log_info!(
            self.ctx,
            "feeder started. shared memory size is {}",
            self.shared_memory.borrow().len()
        );
        let vacant_results =
            BoincDatabase::get_map_keys_by_predicate(&self.db.result.borrow(), |result| {
                result.server_state == ResultState::Unsent && !result.in_shared_mem
            });

        let mut db_result_mut = self.db.result.borrow_mut();

        let mut i: usize = 0;

        while i < vacant_results.len()
            && self.shared_memory.borrow().len() < self.config.shared_memory_size
        {
            let result_id = vacant_results[i];
            let result = db_result_mut.get_mut(&result_id).unwrap();
            self.shared_memory.borrow_mut().push(result_id);
            result.in_shared_mem = true;

            //log_info!(self.ctx, "result {} added to shared memory", result_id);
            i += 1;
        }
        let duration = t.elapsed().as_secs_f64();
        self.dur_sum += duration;
        self.dur_samples += 1;
        // println!("Feeder duration {}", duration);
        // println!(
        //     "Feeder average duration {:.4}",
        //     self.dur_sum / self.dur_samples as f64
        // );
        log_info!(
            self.ctx,
            "feeder finished. shared memory size is {}",
            self.shared_memory.borrow().len()
        );
    }
}
