use dslab_compute::multicore::CoresDependency;
use log::log_enabled;
use log::Level::Info;
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
use crate::common::Start;
use crate::config::sim_config::JobGeneratorConfig;
use crate::simulator::simulator::SetServerIds;

#[derive(Clone, Serialize)]
pub struct ReportStatus {}

#[derive(Clone, Serialize)]
pub struct GenerateJobs {}

pub struct JobGenerator {
    config: JobGeneratorConfig,
    net: Rc<RefCell<Network>>,
    server_id: Option<Id>,
    jobs_generated: u64,
    ctx: SimulationContext,
}

impl JobGenerator {
    pub fn new(
        config: JobGeneratorConfig,
        net: Rc<RefCell<Network>>,
        ctx: SimulationContext,
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
        if log_enabled!(Info) {
            self.ctx
                .emit_self(ReportStatus {}, self.config.report_status_period);
        }
    }

    fn generate_jobs(&mut self) {
        if self.server_id.is_none() {
            return;
        }
        for i in 0..self.config.job_batch_size {
            let job_id = (self.jobs_generated + i as u64) as JobSpecId;
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
                .emit_self(GenerateJobs {}, self.config.job_generation_period);
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
        })
    }
}
