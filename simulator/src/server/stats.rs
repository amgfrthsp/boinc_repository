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
    pub total_credit_granted: f64,
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
            total_credit_granted: 0.,
        }
    }
}

/*
Total number of clients
Average cores
Average speed
Average memory
Average disk

Average hosts availability time %
Average hosts unavailability time %

Average job flops
Average job memory
Average job cores, delay, target_nresults, input size, output size


Workunits waiting for canonical result
Workunits waiting for validation
Workunits waiting for assimilation
Workunits waiting for deletion
Workunits fully processed

Results In progress
Results Over by status
Results outcome %
Results Valid, Invalid

Calculated GFLOPS

Total credit granted:
*/
