use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::rc::Rc;

use crate::server::job::{AssimilateState, ValidateState};

use super::database::BoincDatabase;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct Validator {
    id: Id,
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
}

impl Validator {
    pub fn new(db: Rc<BoincDatabase>, ctx: SimulationContext) -> Self {
        return Self {
            id: ctx.id(),
            db,
            ctx,
        };
    }

    pub fn validate(&self) {
        let workunits_to_validate =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.need_validate == true
            });
        log_info!(self.ctx, "starting validation");
        let mut validated_count = 0;

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        for wu_id in workunits_to_validate {
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();
            let mut canonical_result = None;
            for result_id in &workunit.result_ids {
                let result = db_result_mut.get_mut(&result_id).unwrap();
                result.validate_state = Some(ValidateState::Valid);
                canonical_result = Some(*result_id);
                validated_count += 1;
            }
            workunit.need_validate = false;
            workunit.canonical_resultid = canonical_result;
            if !workunit.canonical_resultid.is_none() {
                workunit.assimilate_state = AssimilateState::Ready;
            }
        }
        log_info!(self.ctx, "validated {} results", validated_count);
    }
}
