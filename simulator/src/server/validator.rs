use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::server::job::{AssimilateState, ValidateState};

use super::job::{ResultInfo, WorkunitInfo};

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct Validator {
    id: Id,
    ctx: SimulationContext,
}

impl Validator {
    pub fn new(ctx: SimulationContext) -> Self {
        return Self { id: ctx.id(), ctx };
    }

    pub fn validate(
        &self,
        workunits_to_validate: Vec<u64>,
        workunits_db: &mut HashMap<u64, Rc<RefCell<WorkunitInfo>>>,
        results_db: &mut HashMap<u64, Rc<RefCell<ResultInfo>>>,
    ) {
        log_info!(self.ctx, "starting validation");
        let mut validated_count = 0;
        for wu_id in workunits_to_validate {
            let mut workunit = workunits_db.get_mut(&wu_id).unwrap().borrow_mut();
            let mut canonical_result = None;
            for result_id in &workunit.result_ids {
                let mut result = results_db.get_mut(&result_id).unwrap().borrow_mut();
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
