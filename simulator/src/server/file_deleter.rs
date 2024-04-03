use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::cell::RefCell;
use std::rc::Rc;

use crate::server::job::FileDeleteState;

use super::data_server::DataServer;
use super::database::BoincDatabase;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct FileDeleter {
    db: Rc<BoincDatabase>,
    data_server: Rc<RefCell<DataServer>>,
    ctx: SimulationContext,
}

impl FileDeleter {
    pub fn new(
        db: Rc<BoincDatabase>,
        data_server: Rc<RefCell<DataServer>>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            db,
            data_server,
            ctx,
        }
    }

    pub fn delete_files(&self) {
        self.delete_input_files();
        self.delete_output_files();
    }

    pub fn delete_input_files(&self) {
        let workunits_to_process =
            BoincDatabase::get_map_keys_by_predicate(&self.db.workunit.borrow(), |wu| {
                wu.file_delete_state == FileDeleteState::Ready
            });

        log_info!(self.ctx, "input file deletion started");

        let mut db_workunit_mut = self.db.workunit.borrow_mut();

        for wu_id in workunits_to_process {
            let workunit = db_workunit_mut.get_mut(&wu_id).unwrap();

            let retval = self.data_server.borrow_mut().delete_input_files(wu_id);
            if retval == 0 {
                workunit.file_delete_state = FileDeleteState::Done;
            }
        }

        log_info!(self.ctx, "input file deletion finished");
    }

    pub fn delete_output_files(&self) {
        let results_to_process =
            BoincDatabase::get_map_keys_by_predicate(&self.db.result.borrow(), |result| {
                result.file_delete_state == FileDeleteState::Ready
            });

        log_info!(self.ctx, "output file deletion started");

        let mut db_result_mut = self.db.result.borrow_mut();

        for result_id in results_to_process {
            let result = db_result_mut.get_mut(&result_id).unwrap();

            let retval = self.data_server.borrow_mut().delete_output_files(result_id);
            if retval == 0 {
                result.file_delete_state = FileDeleteState::Done;
            }
        }

        log_info!(self.ctx, "output file deletion finished");
    }
}
