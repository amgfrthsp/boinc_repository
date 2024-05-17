use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Instant;

use crate::config::sim_config::FeederConfig;
use crate::server::job::ResultState;

use super::database::BoincDatabase;
use super::job::ResultId;

pub struct Feeder {
    shared_memory: Rc<RefCell<VecDeque<ResultId>>>,
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
    config: FeederConfig,
    pub dur_sum: f64,
    dur_samples: usize,
}

impl Feeder {
    pub fn new(
        shared_memory: Rc<RefCell<VecDeque<ResultId>>>,
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
        let mut vacant_results = self.db.feeder_result_ids.borrow_mut();

        let mut db_result_mut = self.db.result.borrow_mut();

        while !vacant_results.is_empty()
            && self.shared_memory.borrow().len() < self.config.shared_memory_size
        {
            let result_id = vacant_results.pop_front().unwrap();
            let result = db_result_mut.get_mut(&result_id).unwrap();
            if !(!result.in_shared_mem && result.server_state == ResultState::Unsent) {
                continue;
            }
            self.shared_memory.borrow_mut().push_back(result_id);
            result.in_shared_mem = true;
        }
        let duration = t.elapsed().as_secs_f64();
        self.dur_sum += duration;
        self.dur_samples += 1;
        log_info!(
            self.ctx,
            "feeder finished. shared memory size is {}",
            self.shared_memory.borrow().len()
        );
    }
}
