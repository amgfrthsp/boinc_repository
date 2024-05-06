use dslab_core::context::SimulationContext;
use dslab_core::log_debug;
use dslab_core::log_info;
use dslab_core::Id;
use std::cell::RefCell;
use std::rc::Rc;

use crate::config::sim_config::ValidatorConfig;
use crate::server::job::{
    AssimilateState, FileDeleteState, ResultOutcome, ResultState, ValidateState,
};

use super::database::BoincDatabase;
use super::job::ResultInfo;
use super::stats::ServerStats;

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
    stats: Rc<RefCell<ServerStats>>,
}

impl Validator {
    pub fn new(
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        config: ValidatorConfig,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        Self {
            db,
            ctx,
            config,
            stats,
        }
    }

    pub fn validate(&self) {
        let workunits_to_validate =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.need_validate
            });
        log_info!(self.ctx, "validation started");

        let mut db_workunit_mut = self.db.workunit.borrow_mut();
        let mut db_result_mut = self.db.result.borrow_mut();

        for wu_id in workunits_to_validate {
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();
            workunit.need_validate = false;
            log_debug!(self.ctx, "validating wu {}", wu_id);

            if workunit.canonical_resultid.is_some() {
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
                        self.stats.borrow_mut().n_results_invalid += 1;
                    } else {
                        self.validate_result(result);
                    }
                }
            } else {
                let mut n_valid_results = 0;
                let mut potential_canonical_result_id = None;
                for result_id in &workunit.result_ids {
                    let result = db_result_mut.get_mut(result_id).unwrap();
                    log_debug!(self.ctx, "validating result {:?}", result);
                    if result.server_state == ResultState::Over
                        && result.outcome == ResultOutcome::Success
                        && result.validate_state == ValidateState::Init
                    {
                        self.validate_result(result);
                    }
                    if result.validate_state == ValidateState::Valid {
                        n_valid_results += 1;
                        potential_canonical_result_id = Some(result.id);
                    }
                }
                log_debug!(self.ctx, "found {} valid results", n_valid_results);
                if n_valid_results >= workunit.spec.min_quorum as usize {
                    if potential_canonical_result_id.is_some() {
                        log_debug!(
                            self.ctx,
                            "found canonical result {} for workunit {}",
                            potential_canonical_result_id.unwrap(),
                            workunit.id,
                        );
                        // grant credit
                        workunit.canonical_resultid = potential_canonical_result_id;
                        workunit.assimilate_state = AssimilateState::Ready;
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

    // todo: add client's reliability parameter & read files from disk
    // Validates results and grants credit if valid
    pub fn validate_result(&self, result: &mut ResultInfo) {
        let new_state;
        if self.ctx.gen_range(0. ..1.) < 0.95 {
            new_state = ValidateState::Valid;
            self.grant_credit(result.client_id, result.claimed_credit);
            self.stats.borrow_mut().n_results_valid += 1;
        } else {
            new_state = ValidateState::Invalid;
            self.stats.borrow_mut().n_results_invalid += 1;
        }
        log_debug!(
            self.ctx,
            "result {} validate_state {:?} -> {:?}",
            result.id,
            result.validate_state,
            new_state,
        );
        result.validate_state = new_state;
    }

    pub fn grant_credit(&self, client_id: Id, credit: f64) {
        let mut clients_ref = self.db.clients.borrow_mut();
        let client = clients_ref.get_mut(&client_id).unwrap();
        log_debug!(
            self.ctx,
            "Client {} credit update: {} + {} = {}",
            client_id,
            client.credit,
            credit,
            client.credit + credit
        );
        client.credit += credit;

        self.stats.borrow_mut().total_credit_granted += credit;
    }
}
