use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info};
use std::cell::RefCell;
use std::rc::Rc;

use crate::config::sim_config::DBPurgerConfig;

use super::database::BoincDatabase;
use super::job::FileDeleteState;
use super::stats::ServerStats;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct DBPurger {
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: DBPurgerConfig,
    stats: Rc<RefCell<ServerStats>>,
}

impl DBPurger {
    pub fn new(
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        config: DBPurgerConfig,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        Self {
            db,
            ctx,
            config,
            stats,
        }
    }

    pub fn purge_database(&self) {
        log_info!(self.ctx, "database purging started");

        let workunits_to_delete =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.file_delete_state == FileDeleteState::Done
            });

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();
        let mut db_processed_result_mut = self.db.processed_results.borrow_mut();

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
                    //db_processed_result_mut.insert(*result_id, result.clone());
                    db_result_mut.remove(result_id);
                    log_debug!(self.ctx, "removed result {} from database", result_id);
                }
            }
            if all_results_deleted {
                db_workunit_mut.remove(&wu_id);
                self.stats.borrow_mut().n_workunits_fully_processed += 1;
                log_debug!(self.ctx, "removed workunit {} from database", wu_id);
            }
        }
        log_info!(self.ctx, "database purging finished");
    }
}
