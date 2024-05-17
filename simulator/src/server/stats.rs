#[derive(Debug, Clone)]
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
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            n_workunits_fully_processed: 0,
            n_results_completed: 0,
            gflops_total: 0.,
            results_processing_time: 0.,
            max_result_processing_time: 0.,
            min_result_processing_time: f64::MAX,
            total_credit_granted: 0.,
            n_res_deleted: 0,
            n_res_success: 0,
            n_res_init: 0,
            n_res_valid: 0,
            n_res_invalid: 0,
            n_res_noreply: 0,
            n_res_didntneed: 0,
            n_res_validateerror: 0,
        }
    }
}
