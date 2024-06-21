use crate::project::job::{InputFileMetadata, OutputFileMetadata};

use crate::project::job::{ResultId, WorkunitId};
use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::RefCell;
use sugars::refcell;

use super::task::ResultInfo;

pub struct FileStorage {
    pub input_files: RefCell<FxHashMap<WorkunitId, InputFileMetadata>>,
    pub output_files: RefCell<FxHashMap<ResultId, OutputFileMetadata>>,
    pub results: RefCell<FxHashMap<ResultId, ResultInfo>>,
    pub results_for_sim: RefCell<FxHashSet<ResultId>>,
    pub running_results: RefCell<FxHashSet<ResultId>>,
}

impl FileStorage {
    pub fn new() -> Self {
        Self {
            input_files: refcell!(FxHashMap::default()),
            output_files: refcell!(FxHashMap::default()),
            results: refcell!(FxHashMap::default()),
            results_for_sim: refcell!(FxHashSet::default()),
            running_results: refcell!(FxHashSet::default()),
        }
    }

    pub fn get_map_keys_by_predicate<K: Clone + Ord, V, F>(
        hm: &FxHashMap<K, V>,
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
