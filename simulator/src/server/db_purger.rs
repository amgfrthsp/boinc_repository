use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

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
    pub dur_sum: f64,
    dur_samples: usize,
}

impl DBPurger {
    pub fn new(
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        Self {
            db,
            ctx,
            stats,
            dur_samples: 0,
            dur_sum: 0.,
        }
    }

    pub fn purge_database(&mut self) {
        let t = Instant::now();
        log_info!(self.ctx, "database purging started");

        let workunits_to_delete =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.file_delete_state == FileDeleteState::Done
            });

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        for wu_id in workunits_to_delete {
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
            }
        }
        log_info!(self.ctx, "database purging finished");

        let duration = t.elapsed().as_secs_f64();
        self.dur_sum += duration;
        self.dur_samples += 1;
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
