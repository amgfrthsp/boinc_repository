use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info};
use std::cell::RefCell;
use std::rc::Rc;

use crate::config::sim_config::AssimilatorConfig;
use crate::server::job::AssimilateState;

use super::database::BoincDatabase;
use super::stats::ServerStats;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct Assimilator {
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: AssimilatorConfig,
    stats: Rc<RefCell<ServerStats>>,
}

impl Assimilator {
    pub fn new(
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        config: AssimilatorConfig,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        Self {
            db,
            ctx,
            config,
            stats,
        }
    }

    pub fn assimilate(&self) {
        let workunits_to_assimilate =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.assimilate_state == AssimilateState::Ready
            });
        log_info!(self.ctx, "assimilation started");
        let mut assimilated_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();

        for wu_id in workunits_to_assimilate {
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();
            log_debug!(
                self.ctx,
                "workunit {} assimilate_state {:?} -> {:?}",
                workunit.id,
                workunit.assimilate_state,
                AssimilateState::Done
            );
            workunit.assimilate_state = AssimilateState::Done;
            assimilated_cnt += 1;
            self.stats.borrow_mut().n_workunits_total += 1;
        }
        log_info!(self.ctx, "assimilated {} workunits", assimilated_cnt);
    }
}
