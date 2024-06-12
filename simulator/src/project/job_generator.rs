use std::cell::RefCell;

use dslab_compute::multicore::CoresDependency;

use dslab_core::{context::SimulationContext, log_info};

use super::job::{InputFileMetadata, JobSpec, JobSpecId, OutputFileMetadata};
use crate::config::sim_config::JobGeneratorConfig;

pub struct JobGenerator {
    jobs_generated: RefCell<u64>,
    ctx: SimulationContext,
    config: JobGeneratorConfig,
}

impl JobGenerator {
    pub fn new(ctx: SimulationContext, config: JobGeneratorConfig) -> Self {
        Self {
            config,
            jobs_generated: RefCell::new(0),
            ctx,
        }
    }

    pub fn generate_jobs(&self, cnt: usize) -> Vec<JobSpec> {
        let mut generated_jobs = Vec::new();
        for i in 0..cnt {
            let job_id = (*self.jobs_generated.borrow() + i as u64) as JobSpecId;
            let job = JobSpec {
                id: job_id,
                gflops: self.config.gflops,
                memory: self.config.memory,
                cores: self.config.cores,
                cores_dependency: CoresDependency::Linear,
                delay_bound: self.config.delay,
                min_quorum: self.config.min_quorum,
                target_nresults: self.config.target_nresults,
                input_file: InputFileMetadata {
                    workunit_id: job_id,
                    size: self.config.input_size,
                },
                output_file: OutputFileMetadata {
                    result_id: 0, // set in scheduler
                    size: self.config.output_size,
                },
            };
            generated_jobs.push(job);

            *self.jobs_generated.borrow_mut() += generated_jobs.len() as u64;
        }

        log_info!(
            self.ctx,
            "Generated {} new workunits {}",
            generated_jobs.len(),
            self.ctx.time()
        );

        generated_jobs
    }
}
