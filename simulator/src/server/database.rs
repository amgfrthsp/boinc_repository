use super::job::{ResultInfo, WorkunitInfo};
use std::{cell::RefCell, collections::HashMap, rc::Rc};

pub struct BoincDatabase {
    pub workunit: HashMap<u64, Rc<RefCell<WorkunitInfo>>>,
    pub result: HashMap<u64, Rc<RefCell<ResultInfo>>>,
}

impl BoincDatabase {
    pub fn new() -> Self {
        return Self {
            workunit: HashMap::new(),
            result: HashMap::new(),
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
