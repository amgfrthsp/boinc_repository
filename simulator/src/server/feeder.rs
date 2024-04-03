use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;

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
}

impl Feeder {
    pub fn new(
        shared_memory: Rc<RefCell<Vec<SharedMemoryItem>>>,
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
    ) -> Self {
        return Self {
            shared_memory,
            db,
            ctx,
        };
    }

    pub fn scan_work_array(&self) {
        log_info!(self.ctx, "feeder scanning started");
        let mut vacant_results =
            BoincDatabase::get_map_keys_by_predicate(&self.db.result.borrow(), |result| {
                result.in_shared_mem == false
            });

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
            result.in_shared_mem = true;

            log_info!(self.ctx, "result {} added to shared memory", result_id);
        }
        log_info!(self.ctx, "feeder scanning finished");
    }
}
