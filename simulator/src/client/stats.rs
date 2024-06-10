use std::ops::AddAssign;

#[derive(Debug, Clone, Default)]
pub struct ClientStats {
    pub n_results_processed: u64,
    pub gflops_processed: f64,
    pub results_processing_time: f64,
    pub max_result_processing_time: f64,
    pub min_result_processing_time: f64,
    pub n_miss_deadline: u64,
    pub time_available: f64,
    pub time_unavailable: f64,

    // component's duration
    pub scheduler_sum_dur: f64,
    pub scheduler_samples: u32,
    pub rrsim_sum_dur: f64,
    pub rrsim_samples: u32,
}

impl ClientStats {
    pub fn new() -> Self {
        Self {
            min_result_processing_time: f64::MAX,
            ..Default::default()
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
    }
}
