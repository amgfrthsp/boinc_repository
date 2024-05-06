use dslab_core::context::SimulationContext;
use dslab_core::log_debug;
use dslab_core::log_info;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::rc::Rc;

use crate::config::sim_config::TransitionerConfig;
use crate::server::job::{
    AssimilateState, FileDeleteState, ResultOutcome, ResultState, ValidateState,
};

use super::job::ResultId;
use super::stats::ServerStats;
use super::{
    database::BoincDatabase,
    job::{ResultInfo, WorkunitInfo},
};

pub struct Transitioner {
    db: Rc<BoincDatabase>,
    next_result_id: ResultId,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: TransitionerConfig,
    stats: Rc<RefCell<ServerStats>>,
}

impl Transitioner {
    pub fn new(
        db: Rc<BoincDatabase>,
        ctx: SimulationContext,
        config: TransitionerConfig,
        stats: Rc<RefCell<ServerStats>>,
    ) -> Self {
        Self {
            db,
            next_result_id: 0,
            ctx,
            config,
            stats,
        }
    }

    pub fn transit(&mut self, current_time: f64) {
        let workunits_to_transit =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                self.ctx.time() >= wu.transition_time
                    && wu.file_delete_state != FileDeleteState::Done
                    && wu.file_delete_state != FileDeleteState::Ready
            });
        log_info!(self.ctx, "transitioning started");

        let mut db_workunit_mut = self.db.workunit.borrow_mut();

        for wu_id in workunits_to_transit {
            // check for timed-out results
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();

            log_debug!(
                self.ctx,
                "transitioning workunit {}: {:?}",
                workunit.id,
                workunit
            );

            let mut next_transition_time = current_time + workunit.spec.delay_bound;
            let mut new_results_needed_cnt: u64 = 0;

            self.check_timed_out_and_validation(
                workunit,
                current_time,
                &mut next_transition_time,
                &mut new_results_needed_cnt,
            );

            self.generate_new_results(workunit, new_results_needed_cnt);

            // self.update_assimilation_state(workunit);

            self.update_file_deletion_state(workunit);

            workunit.transition_time = next_transition_time;

            self.print_statistics_for_workunit(workunit);

            self.next_result_id += new_results_needed_cnt;
        }
        log_info!(self.ctx, "transitioning finished");
    }

    fn check_timed_out_and_validation(
        &self,
        workunit: &mut WorkunitInfo,
        current_time: f64,
        next_transition_time: &mut f64,
        new_results_needed_cnt: &mut u64,
    ) {
        let mut db_result_mut = self.db.result.borrow_mut();

        let mut res_server_state_unsent_cnt = 0; // + inactive
        let mut res_server_state_inprogress_cnt = 0;
        let mut res_outcome_success_cnt = 0;

        let mut need_validate = false;
        *next_transition_time = current_time + workunit.spec.delay_bound;

        for result_id in &workunit.result_ids {
            let result = db_result_mut.get_mut(result_id).unwrap();
            match result.server_state {
                ResultState::Unsent => {
                    res_server_state_unsent_cnt += 1;
                }
                ResultState::InProgress => {
                    if current_time >= result.report_deadline {
                        log_debug!(
                            self.ctx,
                            "result {} server state {:?}, outcome {:?} -> ({:?}, {:?})",
                            result.id,
                            result.server_state,
                            result.outcome,
                            ResultState::Over,
                            ResultOutcome::NoReply,
                        );
                        result.server_state = ResultState::Over;
                        result.outcome = ResultOutcome::NoReply;
                        result.validate_state = ValidateState::Invalid;
                        self.stats.borrow_mut().n_results_invalid += 1;
                        self.stats.borrow_mut().n_miss_deadline += 1;
                    } else {
                        res_server_state_inprogress_cnt += 1;
                        *next_transition_time =
                            f64::min(*next_transition_time, result.report_deadline);
                    }
                }
                ResultState::Over => {
                    if result.outcome == ResultOutcome::Success {
                        if result.validate_state == ValidateState::Init {
                            need_validate = true;
                        }
                        if result.validate_state != ValidateState::Invalid {
                            res_outcome_success_cnt += 1;
                        }
                    }
                }
            }
        }

        // trigger validation if needed
        if need_validate && res_outcome_success_cnt >= workunit.spec.min_quorum {
            workunit.need_validate = true;
        }

        if workunit.canonical_resultid.is_some() {
            *new_results_needed_cnt = 0;
        } else {
            *new_results_needed_cnt = u64::max(
                0,
                workunit
                    .spec
                    .target_nresults
                    .saturating_sub(res_server_state_unsent_cnt)
                    .saturating_sub(res_server_state_inprogress_cnt)
                    .saturating_sub(res_outcome_success_cnt),
            );
        }
    }

    fn generate_new_results(&self, workunit: &mut WorkunitInfo, cnt: u64) {
        // if no WU errors, generate new results if needed

        let mut db_result_mut = self.db.result.borrow_mut();

        for i in 0..cnt {
            let result = ResultInfo {
                id: self.next_result_id + i,
                workunit_id: workunit.id,
                report_deadline: 0.,
                server_state: ResultState::Unsent,
                outcome: ResultOutcome::Undefined,
                validate_state: ValidateState::Init,
                file_delete_state: FileDeleteState::Init,
                in_shared_mem: false,
                time_sent: 0.,
                client_id: 0,
                claimed_credit: 0.,
            };
            workunit.result_ids.push(result.id);
            db_result_mut.insert(result.id, result);

            self.stats.borrow_mut().n_results_total += 1;
        }

        if cnt > 0 {
            log_info!(
                self.ctx,
                "workunit {}: generated {} new results",
                workunit.id,
                cnt
            );
        }
    }

    fn update_file_deletion_state(&self, workunit: &mut WorkunitInfo) {
        // trigger assimilation or file deletion
        let mut db_result_mut = self.db.result.borrow_mut();

        let mut delete_input_files = true;
        let mut delete_canonical_result_files = true;

        for result_id in &workunit.result_ids {
            let result = db_result_mut.get_mut(result_id).unwrap();

            if workunit.canonical_resultid.is_some()
                && *result_id == workunit.canonical_resultid.unwrap()
            {
                continue;
            }

            let mut delete_output_files = true;

            if result.server_state != ResultState::Over {
                delete_input_files = false;
                delete_output_files = false;
                delete_canonical_result_files = false;
            } else {
                if result.outcome == ResultOutcome::Success {
                    if result.validate_state == ValidateState::Init {
                        delete_canonical_result_files = false;
                    }
                }
                if result.validate_state != ValidateState::Valid {
                    delete_output_files = false;
                }
            }

            if workunit.assimilate_state == AssimilateState::Done
                && delete_output_files
                && result.file_delete_state == FileDeleteState::Init
            {
                log_debug!(
                    self.ctx,
                    "result {} file delete state {:?} -> {:?}; wu assimilate_state {:?}",
                    result.id,
                    result.file_delete_state,
                    FileDeleteState::Ready,
                    workunit.assimilate_state,
                );
                result.file_delete_state = FileDeleteState::Ready;
            }
        }

        if workunit.canonical_resultid.is_some()
            && workunit.assimilate_state == AssimilateState::Done
            && delete_canonical_result_files
        {
            let canonical_result = db_result_mut
                .get_mut(&workunit.canonical_resultid.unwrap())
                .unwrap();

            if canonical_result.file_delete_state == FileDeleteState::Init {
                log_debug!(
                    self.ctx,
                    "canonical result {} file delete state {:?} -> {:?}; wu assimilate_state {:?}",
                    canonical_result.id,
                    canonical_result.file_delete_state,
                    FileDeleteState::Ready,
                    workunit.assimilate_state,
                );
                canonical_result.file_delete_state = FileDeleteState::Ready;
            }
        }

        if delete_input_files
            && workunit.assimilate_state == AssimilateState::Done
            && workunit.file_delete_state == FileDeleteState::Init
        {
            log_debug!(
                self.ctx,
                "workunit {} file delete state {:?} -> {:?}",
                workunit.id,
                workunit.file_delete_state,
                FileDeleteState::Ready,
            );
            workunit.file_delete_state = FileDeleteState::Ready;
        }
    }

    fn print_statistics_for_workunit(&self, workunit: &WorkunitInfo) {
        let db_result_mut = self.db.result.borrow_mut();
        log_debug!(
            self.ctx,
            "workunit {}: need_validate: {}; canonical_result_id: {:?}",
            workunit.id,
            workunit.need_validate,
            workunit.canonical_resultid
        );
        log_debug!(
            self.ctx,
            "workunit {}: file_delete_state: {:?}; assimilate_state: {:?}",
            workunit.id,
            workunit.file_delete_state,
            workunit.assimilate_state
        );
        for result_id in &workunit.result_ids {
            if !db_result_mut.contains_key(result_id) {
                log_debug!(
                    self.ctx,
                    "workunit {} result {}: deleted from database",
                    workunit.id,
                    result_id
                );
            } else {
                let result = db_result_mut.borrow().get(result_id).unwrap();
                log_debug!(
                    self.ctx,
                    "workunit {} result {} {:?}: {:?}",
                    workunit.id,
                    result_id,
                    result.server_state,
                    result
                );
            }
        }
    }
}
