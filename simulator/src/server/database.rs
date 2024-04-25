use super::job::{
    AssimilateState, FileDeleteState, ResultId, ResultInfo, ResultState, WorkunitId, WorkunitInfo,
};
use dslab_core::{log_debug, SimulationContext};
use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap},
};
use sugars::refcell;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum DBResultState {
    ServerState { state: ResultState },
    FileDeleteState { state: FileDeleteState },
    InSharedMemoryFlag { flag: bool },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum DBWorkunitState {
    AssimilateState { state: AssimilateState },
    FileDeleteState { state: FileDeleteState },
    NeedValidation { flag: bool },
}

pub struct BoincDatabase {
    pub workunit: RefCell<HashMap<WorkunitId, WorkunitInfo>>,
    pub result: RefCell<HashMap<ResultId, ResultInfo>>,
    res_state2ids: RefCell<HashMap<DBResultState, BTreeSet<ResultId>>>,
    wu_state2ids: RefCell<HashMap<DBWorkunitState, BTreeSet<WorkunitId>>>,
    ctx: SimulationContext,
}

impl BoincDatabase {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            workunit: refcell!(HashMap::new()),
            result: refcell!(HashMap::new()),
            res_state2ids: refcell!(HashMap::new()),
            wu_state2ids: refcell!(HashMap::new()),
            ctx,
        }
    }

    // *********** result index *******************

    pub fn create_new_result(&self, result: ResultInfo) {
        let mut index_ref = self.res_state2ids.borrow_mut();
        // Server state
        let server_state_set = index_ref
            .entry(DBResultState::ServerState {
                state: result.server_state.clone(),
            })
            .or_default();
        server_state_set.insert(result.id);

        // File delete state
        let fd_state_set = index_ref
            .entry(DBResultState::FileDeleteState {
                state: result.file_delete_state.clone(),
            })
            .or_default();
        fd_state_set.insert(result.id);

        // Shared memory state
        let shmem_state_set = index_ref
            .entry(DBResultState::InSharedMemoryFlag {
                flag: result.in_shared_mem,
            })
            .or_default();
        shmem_state_set.insert(result.id);

        log_debug!(self.ctx, "created new result {:?}", result);
        self.result.borrow_mut().insert(result.id, result);
    }

    pub fn process_result_deleted(&self, result: &ResultInfo) {
        let mut index_ref = self.res_state2ids.borrow_mut();
        // Server state
        let server_state_set = index_ref
            .entry(DBResultState::ServerState {
                state: result.server_state.clone(),
            })
            .or_default();
        server_state_set.remove(&result.id);

        // File delete state
        let fd_state_set = index_ref
            .entry(DBResultState::FileDeleteState {
                state: result.file_delete_state.clone(),
            })
            .or_default();
        fd_state_set.remove(&result.id);

        // In shared memory
        let shmem_state_set = index_ref
            .entry(DBResultState::InSharedMemoryFlag {
                flag: result.in_shared_mem,
            })
            .or_default();
        shmem_state_set.remove(&result.id);
    }

    pub fn change_result_state(&self, result: &mut ResultInfo, new_state: DBResultState) {
        let mut index_ref = self.res_state2ids.borrow_mut();

        let old_state_set;
        match new_state.clone() {
            DBResultState::ServerState { state } => {
                log_debug!(
                    self.ctx,
                    "result {} server state change: {:?} -> {:?}",
                    result.id,
                    result.server_state,
                    state
                );
                old_state_set = index_ref
                    .entry(DBResultState::ServerState {
                        state: result.server_state.clone(),
                    })
                    .or_default();
                result.server_state = state;
            }
            DBResultState::FileDeleteState { state } => {
                log_debug!(
                    self.ctx,
                    "result {} file delete state change: {:?} -> {:?}",
                    result.id,
                    result.file_delete_state,
                    state
                );
                old_state_set = index_ref
                    .entry(DBResultState::FileDeleteState {
                        state: result.file_delete_state.clone(),
                    })
                    .or_default();
                result.file_delete_state = state;
            }
            DBResultState::InSharedMemoryFlag { flag } => {
                log_debug!(
                    self.ctx,
                    "result {} in shared memory: {} -> {}",
                    result.id,
                    result.in_shared_mem,
                    flag
                );
                old_state_set = index_ref
                    .entry(DBResultState::InSharedMemoryFlag {
                        flag: result.in_shared_mem,
                    })
                    .or_default();
                result.in_shared_mem = flag;
            }
        }
        old_state_set.remove(&result.id);

        let new_state_set = index_ref.entry(new_state).or_default();
        new_state_set.insert(result.id);
    }

    pub fn get_results_with_state(&self, state: DBResultState) -> Vec<ResultId> {
        let mut index_ref = self.res_state2ids.borrow_mut();
        index_ref
            .entry(state)
            .or_default()
            .clone()
            .into_iter()
            .collect::<Vec<_>>()
    }

    // **************** workunit index ************************

    pub fn create_new_workunit(&self, workunit: WorkunitInfo) {
        let mut index_ref = self.wu_state2ids.borrow_mut();
        // Assimilate state
        let ass_state_set = index_ref
            .entry(DBWorkunitState::AssimilateState {
                state: workunit.assimilate_state.clone(),
            })
            .or_default();
        ass_state_set.insert(workunit.id);

        // File delete state
        let fd_state_set = index_ref
            .entry(DBWorkunitState::FileDeleteState {
                state: workunit.file_delete_state.clone(),
            })
            .or_default();
        fd_state_set.insert(workunit.id);

        // Need validation
        let val_state_set = index_ref
            .entry(DBWorkunitState::NeedValidation {
                flag: workunit.need_validate,
            })
            .or_default();
        val_state_set.insert(workunit.id);

        log_debug!(self.ctx, "created new workunit {:?}", workunit);
        self.workunit.borrow_mut().insert(workunit.id, workunit);
    }

    pub fn process_workunit_deleted(&self, workunit: &WorkunitInfo) {
        let mut index_ref = self.wu_state2ids.borrow_mut();
        // Assimilate state
        let ass_state_set = index_ref
            .entry(DBWorkunitState::AssimilateState {
                state: workunit.assimilate_state.clone(),
            })
            .or_default();
        ass_state_set.remove(&workunit.id);

        // File delete state
        let fd_state_set = index_ref
            .entry(DBWorkunitState::FileDeleteState {
                state: workunit.file_delete_state.clone(),
            })
            .or_default();
        fd_state_set.remove(&workunit.id);

        // Need validation
        let val_state_set = index_ref
            .entry(DBWorkunitState::NeedValidation {
                flag: workunit.need_validate,
            })
            .or_default();
        val_state_set.remove(&workunit.id);
    }

    pub fn change_workunit_state(&self, workunit: &mut WorkunitInfo, new_state: DBWorkunitState) {
        let mut index_ref = self.wu_state2ids.borrow_mut();

        let old_state_set;
        match new_state.clone() {
            DBWorkunitState::AssimilateState { state } => {
                log_debug!(
                    self.ctx,
                    "workunit {} assimilate state change: {:?} -> {:?}",
                    workunit.id,
                    workunit.assimilate_state,
                    state
                );
                old_state_set = index_ref
                    .entry(DBWorkunitState::AssimilateState {
                        state: workunit.assimilate_state.clone(),
                    })
                    .or_default();
                workunit.assimilate_state = state;
            }
            DBWorkunitState::FileDeleteState { state } => {
                log_debug!(
                    self.ctx,
                    "workunit {} file delete state change: {:?} -> {:?}",
                    workunit.id,
                    workunit.file_delete_state,
                    state
                );
                old_state_set = index_ref
                    .entry(DBWorkunitState::FileDeleteState {
                        state: workunit.file_delete_state.clone(),
                    })
                    .or_default();
                workunit.file_delete_state = state;
            }
            DBWorkunitState::NeedValidation { flag } => {
                log_debug!(
                    self.ctx,
                    "workunit {} need validation state change: {} -> {}",
                    workunit.id,
                    workunit.need_validate,
                    flag
                );
                old_state_set = index_ref
                    .entry(DBWorkunitState::NeedValidation {
                        flag: workunit.need_validate,
                    })
                    .or_default();
                workunit.need_validate = flag;
            }
        }
        old_state_set.remove(&workunit.id);

        let new_state_set = index_ref.entry(new_state).or_default();
        new_state_set.insert(workunit.id);
    }

    pub fn get_workunits_with_state(&self, state: DBWorkunitState) -> Vec<WorkunitId> {
        let mut index_ref = self.wu_state2ids.borrow_mut();
        index_ref
            .entry(state)
            .or_default()
            .clone()
            .into_iter()
            .collect::<Vec<_>>()
    }

    pub fn get_map_keys_by_predicate<K: Clone + Ord, V, F>(
        hm: &HashMap<K, V>,
        predicate: F,
    ) -> Vec<K>
    where
        F: Fn(&V) -> bool,
    {
        let mut res = hm
            .iter()
            .filter(|(_, v)| predicate(*v))
            .map(|(k, _)| (*k).clone())
            .collect::<Vec<_>>();
        res.sort();
        res
    }
}
