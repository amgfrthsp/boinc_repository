use dslab_compute::multicore::CoresDependency;
use log::log_enabled;
use log::Level::Info;
use rand::prelude::*;
use rand_pcg::Lcg128Xsl64;
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
use crate::simulator::simulator::SetServerIds;

const BATCH_SIZE: u32 = 3;
const JOBS_AMOUNT_TOTAL: u32 = 5;

#[derive(Clone, Serialize)]
pub struct ReportStatus {}

#[derive(Clone, Serialize)]
pub struct GenerateJobs {}

pub struct JobGenerator {
    rand: Lcg128Xsl64,
    net: Rc<RefCell<Network>>,
    server_id: Option<Id>,
    jobs_generated: u32,
    ctx: SimulationContext,
}

impl JobGenerator {
    pub fn new(rand: Lcg128Xsl64, net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self {
            rand,
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
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
    }

    fn generate_jobs(&mut self) {
        if self.server_id.is_none() {
            return;
        }
        for i in 0..BATCH_SIZE {
            let job_id = (self.jobs_generated + i) as JobSpecId;
            let job = JobSpec {
                id: job_id,
                flops: self.rand.gen_range(100..=1000) as f64,
                memory: self.rand.gen_range(1..=8) * 128,
                min_cores: 1,
                max_cores: 1,
                cores_dependency: CoresDependency::Linear,
                input_file: InputFileMetadata {
                    id: job_id,
                    workunit_id: job_id, // when workunit is created on server, its id equals to job_id
                    size: self.rand.gen_range(100..=1000),
                },
            };
            self.net
                .borrow_mut()
                .send_event(job, self.ctx.id(), self.server_id.unwrap());
        }
        self.jobs_generated += BATCH_SIZE;
        if self.jobs_generated < JOBS_AMOUNT_TOTAL {
            self.ctx.emit_self(GenerateJobs {}, 20.);
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
