use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use dslab_storage::events::DataReadCompleted;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use crate::server::job::AssimilateState;

use super::data_server::DataServer;
use super::database::BoincDatabase;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct Assimilator {
    db: Rc<BoincDatabase>,
    data_server: Rc<RefCell<DataServer>>,
    ctx: SimulationContext,
    pub dur_sum: f64,
    dur_samples: usize,
}

impl Assimilator {
    pub fn new(
        db: Rc<BoincDatabase>,
        data_server: Rc<RefCell<DataServer>>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            db,
            data_server,
            ctx,
            dur_samples: 0,
            dur_sum: 0.,
        }
    }

    pub async fn assimilate(&self) {
        let t = Instant::now();
        let workunits_to_assimilate =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.assimilate_state == AssimilateState::Ready
            });
        log_info!(self.ctx, "assimilation started");
        let mut assimilated_cnt = 0;

        for wu_id in workunits_to_assimilate {
            let mut db_workunit_mut = self.db.workunit.borrow_mut();
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();

            let read_id = self
                .data_server
                .borrow()
                .read_from_disk(workunit.spec.output_file.size, self.ctx.id());

            drop(db_workunit_mut);

            self.ctx
                .recv_event_by_key::<DataReadCompleted>(read_id)
                .await;

            let mut db_workunit_mut = self.db.workunit.borrow_mut();
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();

            log_info!(
                self.ctx,
                "workunit {} assimilate_state {:?} -> {:?}",
                workunit.id,
                workunit.assimilate_state,
                AssimilateState::Done
            );
            workunit.assimilate_state = AssimilateState::Done;
            self.db.update_wu_transition_time(workunit, self.ctx.time());
            assimilated_cnt += 1;
        }
        log_info!(self.ctx, "assimilated {} workunits", assimilated_cnt);
        //let duration = t.elapsed().as_secs_f64();
        // self.dur_sum += duration;
        // self.dur_samples += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use dslab_core::Simulation;
    use dslab_storage::disk::DiskBuilder;

    use crate::server::{
        data_server::DataServer,
        database::BoincDatabase,
        job::{AssimilateState, InputFileMetadata, JobSpec, OutputFileMetadata, WorkunitInfo},
    };

    use super::Assimilator;

    fn default_job_spec() -> JobSpec {
        JobSpec {
            id: 0,
            gflops: 0.,
            memory: 0,
            cores: 0,
            cores_dependency: dslab_compute::multicore::CoresDependency::Linear,
            delay_bound: 0.,
            min_quorum: 0,
            target_nresults: 0,
            input_file: InputFileMetadata::default(),
            output_file: OutputFileMetadata::default(),
        }
    }

    #[test]
    fn test_assimilator() {
        let mut sim = Simulation::new(123);

        sim.step_until_time(10.);

        let db = Rc::new(BoincDatabase::new());

        let wu = WorkunitInfo {
            id: 0,
            spec: default_job_spec(),
            result_ids: Vec::new(),
            client_ids: Vec::new(),
            transition_time: 0.,
            need_validate: false,
            file_delete_state: crate::server::job::FileDeleteState::Init,
            canonical_resultid: Some(0),
            assimilate_state: crate::server::job::AssimilateState::Ready,
        };

        db.insert_new_workunit(wu.clone());

        let disk = Rc::new(RefCell::new(
            DiskBuilder::simple(10000, 10., 10.).build(sim.create_context("disk")),
        ));
        let data_server = Rc::new(RefCell::new(DataServer::new(
            disk.clone(),
            sim.create_context("data_server"),
        )));

        sim.add_handler("data_server", data_server.clone());
        sim.add_handler("disk", disk.clone());

        let assimilator =
            Assimilator::new(db.clone(), data_server, sim.create_context("assimilator"));

        sim.spawn(async move {
            assimilator.assimilate().await;
        });

        sim.step_until_no_events();

        let db_workunit = db.workunit.borrow();

        assert!(db_workunit.contains_key(&wu.id));
        let wu = db_workunit.get(&wu.id).unwrap();
        assert_eq!(wu.assimilate_state, AssimilateState::Done);
        assert_eq!(wu.transition_time, 10.);
    }
}
