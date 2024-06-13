use std::rc::Rc;

use super::server::ProjectServer;

pub struct BoincProject {
    pub name: String,
    pub server: Rc<ProjectServer>,
}

impl BoincProject {
    pub fn new(name: String, server: Rc<ProjectServer>) -> Self {
        Self { name, server }
    }
}
