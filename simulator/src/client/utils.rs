use std::{cell::RefCell, rc::Rc};

use dslab_compute::multicore::Compute;
use dslab_core::{log_debug, SimulationContext};

use super::{
    storage::FileStorage,
    task::{ResultInfo, ResultState},
};

pub struct Utilities {
    compute: Rc<RefCell<Compute>>,
    file_storage: Rc<FileStorage>,
    ctx: SimulationContext,
}

impl Utilities {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        file_storage: Rc<FileStorage>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            compute,
            file_storage,
            ctx,
        }
    }

    // Might be the case when in compute computation is finished and comp_id is deleted
    // But Scheduling started earlier than CompFinished came to client and result's state got updated
    pub fn is_running_finished(&self, result: &ResultInfo) -> bool {
        self.compute
            .borrow()
            .fraction_done(result.comp_id.unwrap())
            .is_err()
    }
}
