use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::log_info;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

// TODO:
// 1. Calculate delay based on output files size
// 2. Split events to simulate a delay

pub struct FileDeleter {
    id: Id,
    ctx: SimulationContext,
}

impl FileDeleter {
    pub fn new(ctx: SimulationContext) -> Self {
        return Self { id: ctx.id(), ctx };
    }

    pub fn delete_files(&self) {}
}
