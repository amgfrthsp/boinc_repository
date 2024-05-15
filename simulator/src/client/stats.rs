use std::ops::AddAssign;

#[derive(Debug, Clone)]
pub struct ClientStats {
    pub n_results_processed: u64,
    pub gflops_processed: f64,
    pub results_processing_time: f64,
    pub max_result_processing_time: f64,
    pub min_result_processing_time: f64,
    pub n_miss_deadline: u64,
    pub time_available: f64,
    pub time_unavailable: f64,
    pub cpu_load_sum: f64,
    pub cpu_load_nsamples: u64,
}

impl ClientStats {
    pub fn new() -> Self {
        Self {
            n_results_processed: 0,
            gflops_processed: 0.,
            results_processing_time: 0.,
            max_result_processing_time: 0.,
            min_result_processing_time: f64::MAX,
            n_miss_deadline: 0,
            time_available: 0.,
            time_unavailable: 0.,
            cpu_load_sum: 0.,
            cpu_load_nsamples: 0,
        }
    }
}

impl AddAssign for ClientStats {
    fn add_assign(&mut self, other: Self) {
        self.n_results_processed += other.n_results_processed;
        self.gflops_processed += other.gflops_processed;
        self.results_processing_time += other.results_processing_time;
        self.max_result_processing_time = self
            .max_result_processing_time
            .max(other.max_result_processing_time);
        self.min_result_processing_time = self
            .min_result_processing_time
            .min(other.min_result_processing_time);
        self.n_miss_deadline += other.n_miss_deadline;
        self.time_available += other.time_available;
        self.time_unavailable += other.time_unavailable;
        self.cpu_load_sum += other.cpu_load_sum;
        self.cpu_load_nsamples += other.cpu_load_nsamples;
    }
}
