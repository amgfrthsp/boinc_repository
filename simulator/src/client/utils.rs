use std::{cell::RefCell, rc::Rc};

use dslab_compute::multicore::Compute;

use super::task::ResultInfo;

pub struct Utilities {
    compute: Rc<RefCell<Compute>>,
}

impl Utilities {
    pub fn new(compute: Rc<RefCell<Compute>>) -> Self {
        Self { compute }
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
