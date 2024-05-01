#[derive(Debug, Clone)]
pub struct ClientStats {
    pub n_results_total: u64,
    pub flops_total: f64,
    pub results_processing_time: f64,
    pub max_result_processing_time: f64,
    pub min_result_processing_time: f64,
    pub n_miss_deadline: u64,
    pub cpu_load_sum: f64,
    pub cpu_load_nsamples: u64,
}
