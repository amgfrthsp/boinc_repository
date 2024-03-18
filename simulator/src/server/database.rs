use super::job::{ResultInfo, WorkunitInfo};
use std::{cell::RefCell, collections::HashMap};
use sugars::refcell;

pub struct BoincDatabase {
    pub workunit: RefCell<HashMap<u64, WorkunitInfo>>,
    pub result: RefCell<HashMap<u64, ResultInfo>>,
}

impl BoincDatabase {
    pub fn new() -> Self {
        return Self {
            workunit: refcell!(HashMap::new()),
            result: refcell!(HashMap::new()),
        };
    }

    pub fn get_map_keys_by_predicate<K: Clone, V, F>(hm: &HashMap<K, V>, predicate: F) -> Vec<K>
    where
        F: Fn(&V) -> bool,
    {
        hm.iter()
            .filter(|(_, v)| predicate(*v))
            .map(|(k, _)| (*k).clone())
            .collect::<Vec<_>>()
    }
}
