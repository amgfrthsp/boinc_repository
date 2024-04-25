use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;

use crate::common::vector_intersection;
use crate::config::sim_config::FeederConfig;
use crate::server::database::DBResultState;
use crate::server::job::ResultState;

use super::database::BoincDatabase;
use super::job::{ResultId, WorkunitId};

#[derive(Serialize, Debug, Clone, PartialEq)]
pub enum SharedMemoryItemState {
    Empty,
    Present,
}

#[derive(Serialize, Debug, Clone)]
pub struct SharedMemoryItem {
    pub state: SharedMemoryItemState,
    pub result_id: ResultId,
    pub workunit_id: WorkunitId,
}

pub struct Feeder {
    shared_memory: Rc<RefCell<Vec<SharedMemoryItem>>>,
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: FeederConfig,
}

impl Feeder {
    pub fn new(
        shared_memory: Rc<RefCell<Vec<SharedMemoryItem>>>,
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        config: FeederConfig,
    ) -> Self {
        Self {
            shared_memory,
            db,
            ctx,
            config,
        }
    }

    pub fn scan_work_array(&self) {
        log_info!(self.ctx, "feeder scanning started");

        let unsent_results = self.db.get_results_with_state(DBResultState::ServerState {
            state: ResultState::Unsent,
        });
        let not_in_shmem_results = self
            .db
            .get_results_with_state(DBResultState::InSharedMemoryFlag { flag: false });
        let mut vacant_results = vector_intersection(&unsent_results, &not_in_shmem_results);

        let mut db_result_mut = self.db.result.borrow_mut();
        let n = self.shared_memory.as_ref().borrow().len();

        for i in 0..n {
            if vacant_results.is_empty() {
                break;
            }
            if self.shared_memory.borrow()[i].state == SharedMemoryItemState::Present {
                continue;
            }
            let result_id = vacant_results.pop().unwrap();
            let result = db_result_mut.get_mut(&result_id).unwrap();
            self.shared_memory.borrow_mut()[i] = SharedMemoryItem {
                state: SharedMemoryItemState::Present,
                result_id,
                workunit_id: result.workunit_id,
            };
            self.db
                .change_result_state(result, DBResultState::InSharedMemoryFlag { flag: true });

            log_info!(self.ctx, "result {} added to shared memory", result_id);
        }
        log_info!(self.ctx, "feeder scanning finished");
    }
}
