use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::server::job::AssimilateState;

use super::job::WorkunitInfo;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct Assimilator {
    id: Id,
    ctx: SimulationContext,
}

impl Assimilator {
    pub fn new(ctx: SimulationContext) -> Self {
        return Self { id: ctx.id(), ctx };
    }

    pub fn assimilate(
        &self,
        workunits_to_assimilate: Vec<u64>,
        workunits_db: &mut HashMap<u64, Rc<RefCell<WorkunitInfo>>>,
    ) {
        log_info!(self.ctx, "starting assimilation");
        let mut assimilated_cnt = 0;
        for wu_id in workunits_to_assimilate {
            let mut workunit = workunits_db.get_mut(&wu_id).unwrap().borrow_mut();
            workunit.assimilate_state = AssimilateState::Done;
            assimilated_cnt += 1;
        }
        log_info!(self.ctx, "assimilated {} workunits", assimilated_cnt);
    }
}
