use dslab_compute::multicore::CoresDependency;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug};
use dslab_network::Network;

use super::job::{InputFileMetadata, JobSpec, JobSpecId};
use crate::common::{ReportStatus, Start};
use crate::config::sim_config::JobGeneratorConfig;
use crate::simulator::simulator::SetServerIds;

#[derive(Clone, Serialize)]
pub struct GenerateJobs {}

pub struct JobGenerator {
    net: Rc<RefCell<Network>>,
    server_id: Option<Id>,
    jobs_generated: u64,
    ctx: SimulationContext,
    #[allow(dead_code)]
    config: JobGeneratorConfig,
}

impl JobGenerator {
    pub fn new(
        net: Rc<RefCell<Network>>,
        ctx: SimulationContext,
        config: JobGeneratorConfig,
    ) -> Self {
        Self {
            config,
            net,
            server_id: None,
            jobs_generated: 0,
            ctx,
        }
    }

    fn on_started(&mut self) {
        log_debug!(self.ctx, "started");
        self.ctx.emit_self(GenerateJobs {}, 1.);
        self.ctx
            .emit_self(ReportStatus {}, self.config.report_status_interval);
    }

    fn generate_jobs(&mut self) {
        if self.server_id.is_none() {
            return;
        }
        for i in 0..self.config.job_batch_size {
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
            };
            self.net
                .borrow_mut()
                .send_event(job, self.ctx.id(), self.server_id.unwrap());
        }
        self.jobs_generated += self.config.job_batch_size;
        if self.jobs_generated < self.config.job_count {
            self.ctx
                .emit_self(GenerateJobs {}, self.config.job_generation_interval);
        }
    }
}

impl EventHandler for JobGenerator {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            SetServerIds { server_id, .. } => {
                self.server_id = Some(server_id);
            }
            Start {} => {
                self.on_started();
            }
            GenerateJobs {} => {
                self.generate_jobs();
            }
            ReportStatus {} => {}
        })
    }
}
