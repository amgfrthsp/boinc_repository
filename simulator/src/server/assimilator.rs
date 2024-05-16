use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

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
    pub dur_sum: f64,
    dur_samples: usize,
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
            dur_samples: 0,
            dur_sum: 0.,
        }
    }

    pub fn assimilate(&mut self) {
        let t = Instant::now();
        let workunits_to_assimilate =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.assimilate_state == AssimilateState::Ready
            });
        log_info!(self.ctx, "assimilation started");
        let mut assimilated_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();

        for wu_id in workunits_to_assimilate {
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();
            log_info!(
                self.ctx,
                "workunit {} assimilate_state {:?} -> {:?}",
                workunit.id,
                workunit.assimilate_state,
                AssimilateState::Done
            );
            workunit.assimilate_state = AssimilateState::Done;
            self.db.update_wu_transition_time(workunit, self.ctx.time());
            assimilated_cnt += 1;
        }
        let duration = t.elapsed().as_secs_f64();
        log_info!(self.ctx, "assimilated {} workunits", assimilated_cnt);
        self.dur_sum += duration;
        self.dur_samples += 1;
    }
}
