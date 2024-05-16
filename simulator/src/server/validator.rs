use dslab_core::context::SimulationContext;
use dslab_core::log_debug;
use dslab_core::log_info;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use crate::config::sim_config::ValidatorConfig;
use crate::server::job::{
    AssimilateState, FileDeleteState, ResultOutcome, ResultState, ValidateState,
};

use super::database::BoincDatabase;
use super::database::ClientInfo;
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
    pub dur_sum: f64,
    dur_samples: usize,
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
            dur_samples: 0,
            dur_sum: 0.,
        }
    }

    pub fn validate(&mut self) {
        let t = Instant::now();
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
                    if !db_result_mut.contains_key(result_id) {
                        continue;
                    }
                    let result = db_result_mut.get_mut(result_id).unwrap();
                    if !(result.server_state == ResultState::Over
                        && result.outcome == ResultOutcome::Success
                        && result.validate_state == ValidateState::Init)
                    {
                        continue;
                    }
                    if canonical_result_delete_state == FileDeleteState::Done {
                        log_info!(
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
                        self.validate_result(result);
                    }
                }
            } else {
                let mut n_valid_results = 0;
                let mut potential_canonical_result_id = None;
                for result_id in &workunit.result_ids {
                    let result = db_result_mut.get_mut(result_id).unwrap();
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
                if n_valid_results >= (workunit.spec.min_quorum / 2 + 1) as usize {
                    if potential_canonical_result_id.is_some() {
                        log_info!(
                            self.ctx,
                            "found canonical result {} for workunit {}",
                            potential_canonical_result_id.unwrap(),
                            workunit.id,
                        );
                        workunit.canonical_resultid = potential_canonical_result_id;
                        workunit.assimilate_state = AssimilateState::Ready;
                        for result_id in &workunit.result_ids {
                            let result = db_result_mut.get_mut(result_id).unwrap();
                            if result.server_state == ResultState::Unsent {
                                log_info!(
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
                                result.file_delete_state = FileDeleteState::Done;
                                result.in_shared_mem = false;
                            }
                        }
                    }
                }
            }
            self.db.update_wu_transition_time(workunit, self.ctx.time());
        }
        log_info!(self.ctx, "validation finished");
        let duration = t.elapsed().as_secs_f64();
        self.dur_sum += duration;
        self.dur_samples += 1;
    }

    // todo: read files from disk
    // Validates results and grants credit if valid
    pub fn validate_result(&self, result: &mut ResultInfo) {
        let mut clients_ref = self.db.clients.borrow_mut();
        let client = clients_ref.get_mut(&result.client_id).unwrap();

        let new_state;
        if result.is_correct {
            new_state = ValidateState::Valid;
            self.grant_credit(client, result.claimed_credit);
        } else {
            new_state = ValidateState::Invalid;
        }
        log_info!(
            self.ctx,
            "result {} validate_state {:?} -> {:?}",
            result.id,
            result.validate_state,
            new_state,
        );
        result.validate_state = new_state;
    }

    pub fn grant_credit(&self, client: &mut ClientInfo, credit: f64) {
        log_info!(
            self.ctx,
            "Client {} credit update: {} + {} = {}",
            client.id,
            client.credit,
            credit,
            client.credit + credit
        );
        client.credit += credit;

        self.stats.borrow_mut().total_credit_granted += credit;
    }
}
