use dslab_core::context::SimulationContext;
use dslab_core::log_debug;
use dslab_core::log_info;
use std::rc::Rc;

use crate::config::sim_config::ValidatorConfig;
use crate::server::database::DBWorkunitState;
use crate::server::job::{
    AssimilateState, FileDeleteState, ResultOutcome, ResultState, ValidateState,
};

use super::database::BoincDatabase;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

#[derive(Debug, Clone, PartialEq)]
pub enum TransitionTime {
    Never,
    Now,
    Delayed,
}

pub struct Validator {
    db: Rc<BoincDatabase>,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: ValidatorConfig,
}

impl Validator {
    pub fn new(db: Rc<BoincDatabase>, ctx: SimulationContext, config: ValidatorConfig) -> Self {
        Self { db, ctx, config }
    }

    pub fn validate(&self) {
        let workunits_to_validate = self
            .db
            .get_workunits_with_state(DBWorkunitState::NeedValidation { flag: true });
        // let workunits_to_validate_old =
        //     BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
        //         wu.need_validate
        //     });

        // log_debug!(self.ctx, "SCAN: {:?}", workunits_to_validate_old);
        // log_debug!(self.ctx, "INDEX: {:?}", workunits_to_validate);

        // assert_eq!(workunits_to_validate, workunits_to_validate_old);
        log_info!(self.ctx, "validation started");

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        for wu_id in workunits_to_validate {
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();
            self.db
                .change_workunit_state(workunit, DBWorkunitState::NeedValidation { flag: false });

            if workunit.canonical_resultid.is_some() {
                // canonical result is found. grant credit or validate
                let canonical_result_delete_state = db_result_mut
                    .get(&workunit.canonical_resultid.unwrap())
                    .unwrap()
                    .file_delete_state;
                for result_id in &workunit.result_ids {
                    let result = db_result_mut.get_mut(result_id).unwrap();
                    if !(result.server_state == ResultState::Over
                        && result.outcome == ResultOutcome::Success
                        && result.validate_state == ValidateState::Init)
                    {
                        continue;
                    }
                    if canonical_result_delete_state == FileDeleteState::Done {
                        log_debug!(
                            self.ctx,
                            "result {} (outcome {:?}, validate_state {:?}) -> ({:?}, {:?})",
                            result.id,
                            result.outcome,
                            result.validate_state,
                            ResultOutcome::ValidateError,
                            ValidateState::Invalid,
                        );
                        result.outcome = ResultOutcome::ValidateError;
                        result.validate_state = ValidateState::Invalid;
                    } else {
                        // todo: add random & grant credit
                        log_debug!(
                            self.ctx,
                            "result {} validate_state {:?} -> {:?}",
                            result.id,
                            result.validate_state,
                            ValidateState::Valid,
                        );
                        result.validate_state = ValidateState::Valid;
                    }
                }
            } else {
                let mut viable_results = vec![];
                for result_id in &workunit.result_ids {
                    let result = db_result_mut.get_mut(result_id).unwrap();
                    if !(result.server_state == ResultState::Over
                        && result.outcome == ResultOutcome::Success
                        && result.validate_state != ValidateState::Invalid)
                    {
                        continue;
                    }
                    viable_results.push(*result_id);
                }
                if viable_results.len() >= workunit.spec.min_quorum as usize {
                    let mut canonical_result_id = None;

                    // todo: check_set function implement according to boinc
                    for result_id in viable_results {
                        let result = db_result_mut.get_mut(&result_id).unwrap();
                        log_debug!(
                            self.ctx,
                            "result {} validate_state {:?} -> {:?}",
                            result.id,
                            result.validate_state,
                            ValidateState::Valid,
                        );
                        result.validate_state = ValidateState::Valid;
                        canonical_result_id = Some(result_id);
                    }

                    if canonical_result_id.is_some() {
                        log_debug!(
                            self.ctx,
                            "found canonical result {} for workunit {}",
                            canonical_result_id.unwrap(),
                            workunit.id,
                        );
                        // grant credit
                        workunit.canonical_resultid = canonical_result_id;
                        self.db.change_workunit_state(
                            workunit,
                            DBWorkunitState::AssimilateState {
                                state: AssimilateState::Ready,
                            },
                        );
                        for result_id in &workunit.result_ids {
                            let result = db_result_mut.get_mut(result_id).unwrap();
                            if result.server_state == ResultState::Unsent {
                                log_debug!(
                                    self.ctx,
                                    "result {} (server_state {:?}, outcome {:?}) -> ({:?}, {:?})",
                                    result.id,
                                    result.server_state,
                                    result.outcome,
                                    ResultState::Over,
                                    ResultOutcome::DidntNeed,
                                );
                                result.server_state = ResultState::Over;
                                result.outcome = ResultOutcome::DidntNeed;
                            }
                        }
                    } else {
                        // set errors & update target_nresults if needed
                    }
                }
            }
            // TransitionTime::Now
            workunit.transition_time = self.ctx.time();
        }
        log_info!(self.ctx, "validation finished");
    }
}
