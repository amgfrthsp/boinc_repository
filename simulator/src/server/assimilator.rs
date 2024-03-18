use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::rc::Rc;

use crate::server::job::AssimilateState;

use super::database::BoincDatabase;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct Assimilator {
    id: Id,
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
}

impl Assimilator {
    pub fn new(db: Rc<BoincDatabase>, ctx: SimulationContext) -> Self {
        return Self {
            id: ctx.id(),
            db,
            ctx,
        };
    }

    pub fn assimilate(&self) {
        let workunits_to_assimilate =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.assimilate_state == AssimilateState::Ready
            });
        log_info!(self.ctx, "starting assimilation");
        let mut assimilated_cnt = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();

        for wu_id in workunits_to_assimilate {
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();
            workunit.assimilate_state = AssimilateState::Done;
            assimilated_cnt += 1;
        }
        log_info!(self.ctx, "assimilated {} workunits", assimilated_cnt);
    }
}
