#[derive(Debug, Clone)]
pub struct ServerStats {
    pub n_workunits_fully_processed: u64,
    pub n_results_completed: u64,
    pub flops_total: f64,
    pub results_processing_time: f64,
    pub max_result_processing_time: f64,
    pub min_result_processing_time: f64,
    pub total_credit_granted: f64,
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            n_workunits_fully_processed: 0,
            n_results_completed: 0,
            flops_total: 0.,
            results_processing_time: 0.,
            max_result_processing_time: 0.,
            min_result_processing_time: f64::MAX,
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

*/
