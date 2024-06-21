use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info};
use std::cell::RefCell;
use std::rc::Rc;

use super::database::BoincDatabase;
use super::job::{FileDeleteState, ResultInfo, ResultOutcome, ValidateState};
use super::stats::ServerStats;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct DBPurger {
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
    stats: Rc<RefCell<ServerStats>>,
}

impl DBPurger {
    pub fn new(
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        Self { db, ctx, stats }
    }

    pub fn purge_database(&self) {
        log_info!(self.ctx, "database purging started");

        let mut workunits_to_delete = self.db.workunits_to_delete.borrow_mut();

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        let len = workunits_to_delete.len();
        let mut i = 0;

        loop {
            if i >= len {
                break;
            }
            i += 1;
            let wu_id = workunits_to_delete.pop_front().unwrap();
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();
            let mut all_results_deleted = true;
            for result_id in &workunit.result_ids {
                if !db_result_mut.contains_key(result_id) {
                    continue;
                }
                let result = db_result_mut.get(result_id).unwrap();
                if result.file_delete_state != FileDeleteState::Done {
                    all_results_deleted = false;
                } else {
                    self.update_stats(db_result_mut.remove(result_id).unwrap());
                    log_debug!(self.ctx, "removed result {} from database", result_id);
                }
            }
            if all_results_deleted {
                self.db.no_transition_needed(workunit);
                db_workunit_mut.remove(&wu_id);
                self.stats.borrow_mut().n_workunits_fully_processed += 1;
                log_debug!(self.ctx, "removed workunit {} from database", wu_id);
            } else {
                workunits_to_delete.push_back(wu_id);
            }
        }
        log_info!(self.ctx, "database purging finished");
    }

    pub fn update_stats(&self, result: ResultInfo) {
        self.stats.borrow_mut().n_res_deleted += 1;
        match result.outcome {
            ResultOutcome::Undefined => {}
            ResultOutcome::Success => {
                self.stats.borrow_mut().n_res_success += 1;
                match result.validate_state {
                    ValidateState::Valid => {
                        self.stats.borrow_mut().n_res_valid += 1;
                    }
                    ValidateState::Invalid => {
                        self.stats.borrow_mut().n_res_invalid += 1;
                    }
                    ValidateState::Init => {
                        self.stats.borrow_mut().n_res_init += 1;
                    }
                }
            }
            ResultOutcome::NoReply => {
                self.stats.borrow_mut().n_res_noreply += 1;
            }
            ResultOutcome::DidntNeed => {
                self.stats.borrow_mut().n_res_didntneed += 1;
            }
            ResultOutcome::ValidateError => {
                self.stats.borrow_mut().n_res_validateerror += 1;
            }
        }
    }
}
