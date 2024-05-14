use super::job::{ResultId, ResultInfo, WorkunitId, WorkunitInfo};
use dslab_core::Id;
use std::{cell::RefCell, collections::HashMap};
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
    pub result: RefCell<HashMap<ResultId, ResultInfo>>,
    pub processed_results: RefCell<HashMap<ResultId, ResultInfo>>,
    pub clients: RefCell<HashMap<Id, ClientInfo>>,
}

impl BoincDatabase {
    pub fn new() -> Self {
        Self {
            workunit: refcell!(HashMap::new()),
            result: refcell!(HashMap::new()),
            processed_results: refcell!(HashMap::new()),
            clients: refcell!(HashMap::new()),
        }
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
