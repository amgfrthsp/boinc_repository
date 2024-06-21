#[derive(Debug, Clone, Default)]
pub struct ServerStats {
    pub n_workunits_fully_processed: u64,
    pub n_results_completed: u64,
    pub gflops_total: f64,
    pub results_processing_time: f64,
    pub max_result_processing_time: f64,
    pub min_result_processing_time: f64,
    pub total_credit_granted: f64,

    // fully processed results
    pub n_res_deleted: usize,
    pub n_res_success: usize,
    pub n_res_init: usize,
    pub n_res_valid: usize,
    pub n_res_invalid: usize,
    pub n_res_noreply: usize,
    pub n_res_didntneed: usize,
    pub n_res_validateerror: usize,

    // component's duration
    pub assimilator_sum_dur: f64,
    pub assimilator_samples: u32,
    pub db_purger_sum_dur: f64,
    pub db_purger_samples: u32,
    pub feeder_sum_dur: f64,
    pub feeder_samples: u32,
    pub file_deleter_sum_dur: f64,
    pub file_deleter_samples: u32,
    pub scheduler_sum_dur: f64,
    pub scheduler_samples: u32,
    pub scheduler_shmem_empty: u32,
    pub transitioner_sum_dur: f64,
    pub transitioner_samples: u32,
    pub validator_sum_dur: f64,
    pub validator_samples: u32,
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            min_result_processing_time: f64::MAX,
            ..Default::default()
        }
    }
}
