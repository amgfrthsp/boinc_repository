use crate::server::job::{InputFileMetadata, OutputFileMetadata};

use crate::server::job::{ResultId, WorkunitId};
use dslab_core::EventId;
use std::collections::HashSet;
use std::{cell::RefCell, collections::HashMap};
use sugars::refcell;

use super::task::ResultInfo;

pub struct FileStorage {
    pub input_files: RefCell<HashMap<WorkunitId, InputFileMetadata>>,
    pub output_files: RefCell<HashMap<ResultId, OutputFileMetadata>>,
    pub results: RefCell<HashMap<ResultId, ResultInfo>>,
    pub running_results: RefCell<HashSet<(ResultId, EventId)>>,
}

impl FileStorage {
    pub fn new() -> Self {
        Self {
            input_files: refcell!(HashMap::new()),
            output_files: refcell!(HashMap::new()),
            results: refcell!(HashMap::new()),
            running_results: refcell!(HashSet::new()),
        }
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
