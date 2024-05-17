use crate::common::FloatWrapper;

use super::job::{ResultId, ResultInfo, WorkunitId, WorkunitInfo};
use dslab_core::Id;
use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap, VecDeque},
};
use sugars::refcell;

#[derive(Debug)]
pub struct ClientInfo {
    pub id: Id,
    pub speed: f64,
    pub cores: u32,
    pub memory: u64,
    pub credit: f64,
}

pub struct BoincDatabase {
    pub workunit: RefCell<HashMap<WorkunitId, WorkunitInfo>>,
    pub transition_time_sorted: RefCell<BTreeSet<(FloatWrapper, WorkunitId)>>,
    pub result: RefCell<HashMap<ResultId, ResultInfo>>,
    pub feeder_result_ids: RefCell<VecDeque<ResultId>>,
    pub clients: RefCell<HashMap<Id, ClientInfo>>,
}

impl BoincDatabase {
    pub fn new() -> Self {
        Self {
            workunit: refcell!(HashMap::new()),
            result: refcell!(HashMap::new()),
            feeder_result_ids: refcell!(VecDeque::new()),
            transition_time_sorted: refcell!(BTreeSet::new()),
            clients: refcell!(HashMap::new()),
        }
    }

    pub fn update_wu_transition_time(&self, wu: &mut WorkunitInfo, t_time: f64) {
        self.transition_time_sorted
            .borrow_mut()
            .remove(&(FloatWrapper(wu.transition_time), wu.id));
        wu.transition_time = t_time;
        self.transition_time_sorted
            .borrow_mut()
            .insert((FloatWrapper(wu.transition_time), wu.id));
    }

    pub fn insert_new_workunit(&self, wu: WorkunitInfo) {
        self.transition_time_sorted
            .borrow_mut()
            .insert((FloatWrapper(wu.transition_time), wu.id));
        self.workunit.borrow_mut().insert(wu.id, wu);
    }

    pub fn no_transition_needed(&self, wu: &WorkunitInfo) {
        self.transition_time_sorted
            .borrow_mut()
            .remove(&(FloatWrapper(wu.transition_time), wu.id));
    }

    pub fn get_wus_for_transitioner(&self, pivot_time: f64) -> Vec<WorkunitId> {
        let tt_sorted = self.transition_time_sorted.borrow();
        let mut wus_to_transit = Vec::new();
        for (time, id) in tt_sorted.iter() {
            if time.0 > pivot_time {
                break;
            }
            wus_to_transit.push(*id);
        }
        wus_to_transit
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
