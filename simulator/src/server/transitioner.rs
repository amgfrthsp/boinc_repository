use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::server::job::{ResultOutcome, ResultState, ValidateState};

use super::job::{ResultInfo, WorkunitInfo};

// TODO:

pub struct Transitioner {
    id: Id,
    ctx: SimulationContext,
}

impl Transitioner {
    pub fn new(ctx: SimulationContext) -> Self {
        return Self { id: ctx.id(), ctx };
    }

    pub fn transit(
        &self,
        workunits_to_transit: Vec<u64>,
        workunits_db: &mut HashMap<u64, Rc<RefCell<WorkunitInfo>>>,
        results_db: &mut HashMap<u64, Rc<RefCell<ResultInfo>>>,
        current_time: f64,
    ) {
        log_info!(self.ctx, "starting transitioning");
        for wu_id in workunits_to_transit {
            // check for timed-out results
            let mut workunit = workunits_db.get_mut(&wu_id).unwrap().borrow_mut();

            let mut res_server_state_unsent_cnt = 0; // + inactive
            let mut res_server_state_inprogress_cnt = 0;
            let mut res_outcome_success_cnt = 0;

            let mut need_validate = false;
            let mut next_transition_time = current_time + workunit.delay_bound;

            for result_id in &workunit.result_ids {
                let mut result = results_db.get_mut(result_id).unwrap().borrow_mut();
                match result.server_state {
                    ResultState::Inactive => {
                        res_server_state_unsent_cnt += 1;
                    }
                    ResultState::Unsent => {
                        res_server_state_unsent_cnt += 1;
                    }
                    ResultState::InProgress => {
                        if current_time >= result.report_deadline {
                            result.server_state = ResultState::Over;
                            result.outcome = Some(ResultOutcome::NoReply);
                        } else {
                            res_server_state_inprogress_cnt += 1;
                            next_transition_time =
                                f64::min(next_transition_time, result.report_deadline);
                        }
                    }
                    ResultState::Over => match result.outcome {
                        Some(ResultOutcome::Success) => {
                            match result.validate_state {
                                Some(ValidateState::Init) => {
                                    need_validate = true;
                                }
                                _ => {}
                            }
                            if !matches!(result.validate_state, Some(ValidateState::Invalid)) {
                                res_outcome_success_cnt += 1;
                            }
                        }
                        _ => {}
                    },
                }
            }

            log_info!(
                self.ctx,
                "workunit {}: UNSENT {} / IN PROGRESS {} / SUCCESS {}",
                wu_id,
                res_server_state_unsent_cnt,
                res_server_state_inprogress_cnt,
                res_outcome_success_cnt
            );
            // trigger validation if needed
            if need_validate && res_outcome_success_cnt >= workunit.min_quorum {
                workunit.need_validate = true;
            }
            // if no WU errors, generate new results if needed
            let new_results_needed_cnt = u64::max(
                0,
                workunit
                    .target_nresults
                    .saturating_sub(res_server_state_unsent_cnt)
                    .saturating_sub(res_server_state_inprogress_cnt)
                    .saturating_sub(res_outcome_success_cnt),
            );
            for _ in 0..new_results_needed_cnt {
                let result = ResultInfo {
                    id: results_db.len() as u64,
                    workunit_id: workunit.id,
                    report_deadline: 0.,
                    server_state: ResultState::Unsent,
                    outcome: None,
                    validate_state: None,
                };
                workunit.result_ids.push(result.id);
                results_db.insert(result.id, Rc::new(RefCell::new(result)));
            }
            workunit.transition_time = next_transition_time;
            if new_results_needed_cnt > 0 {
                log_info!(
                    self.ctx,
                    "workunit {}: generated {} new results",
                    wu_id,
                    new_results_needed_cnt
                );
            }
        }
    }
}
