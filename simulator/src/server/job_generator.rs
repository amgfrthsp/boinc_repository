use dslab_compute::multicore::CoresDependency;

use dslab_core::{context::SimulationContext, log_debug};

use super::job::{InputFileMetadata, JobSpec, JobSpecId, OutputFileMetadata};
use crate::config::sim_config::JobGeneratorConfig;

pub struct JobGenerator {
    jobs_generated: u64,
    ctx: SimulationContext,
    config: JobGeneratorConfig,
}

impl JobGenerator {
    pub fn new(ctx: SimulationContext, config: JobGeneratorConfig) -> Self {
        Self {
            config,
            jobs_generated: 0,
            ctx,
        }
    }

    pub fn generate_jobs(&mut self, cnt: usize) -> Vec<JobSpec> {
        let mut generated_jobs = Vec::new();
        for i in 0..cnt {
            let job_id = (self.jobs_generated + i as u64) as JobSpecId;
            let min_quorum = self
                .ctx
                .gen_range(self.config.min_quorum_lower_bound..=self.config.min_quorum_upper_bound);
            let job = JobSpec {
                id: job_id,
                flops: self
                    .ctx
                    .gen_range(self.config.flops_lower_bound..=self.config.flops_upper_bound),
                memory: self
                    .ctx
                    .gen_range(self.config.memory_lower_bound..=self.config.memory_upper_bound)
                    * 128,
                cores: self
                    .ctx
                    .gen_range(self.config.cores_lower_bound..=self.config.cores_upper_bound),
                cores_dependency: CoresDependency::Linear,
                delay_bound: self
                    .ctx
                    .gen_range(self.config.delay_lower_bound..=self.config.delay_upper_bound),
                min_quorum,
                target_nresults: min_quorum
                    + self
                        .ctx
                        .gen_range(0..=self.config.target_nresults_redundancy),
                input_file: InputFileMetadata {
                    workunit_id: job_id, // when workunit is created on server, its id equals to job_id
                    size: self.ctx.gen_range(
                        self.config.input_size_lower_bound..=self.config.input_size_upper_bound,
                    ),
                },
                output_file: OutputFileMetadata {
                    result_id: 0, // set in scheduler
                    size: self.ctx.gen_range(
                        self.config.output_size_lower_bound..=self.config.output_size_upper_bound,
                    ),
                },
            };
            generated_jobs.push(job);
        }

        log_debug!(self.ctx, "Generated {} new workunits", generated_jobs.len());

        self.jobs_generated += generated_jobs.len() as u64;

        generated_jobs
    }
}
