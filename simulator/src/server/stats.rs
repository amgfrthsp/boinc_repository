#[derive(Debug, Clone)]
pub struct ServerStats {
    pub n_workunits_total: u64,
    pub n_results_total: u64,
    pub flops_total: f64,
    pub results_processing_time: f64,
    pub max_result_processing_time: f64,
    pub min_result_processing_time: f64,
    pub n_miss_deadline: u64,
    pub n_results_valid: u64,
    pub n_results_invalid: u64,
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            n_workunits_total: 0,
            n_results_total: 0,
            flops_total: 0.,
            results_processing_time: 0.,
            max_result_processing_time: 0.,
            min_result_processing_time: 0.,
            n_miss_deadline: 0,
            n_results_valid: 0,
            n_results_invalid: 0,
        }
    }
}
