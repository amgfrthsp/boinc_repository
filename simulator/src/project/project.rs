use std::{cell::RefCell, rc::Rc};

use super::server::ProjectServer;

pub struct BoincProject {
    pub name: String,
    pub server: Rc<RefCell<ProjectServer>>,
}

impl BoincProject {
    pub fn new(name: String, server: Rc<RefCell<ProjectServer>>) -> Self {
        Self { name, server }
    }
}
