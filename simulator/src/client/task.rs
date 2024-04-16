use dslab_core::EventId;

use crate::server::job::{JobSpec, OutputFileMetadata};

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ResultState {
    Downloading,
    ReadyToExecute,
    Running,
    Preempted,
    ReadyToUpload,
    ReadyToNotify,
    Over,
}

#[derive(Debug, Clone)]
pub struct ResultInfo {
    pub spec: JobSpec,
    pub output_file: OutputFileMetadata,
    pub state: ResultState,
    pub comp_id: Option<EventId>,
}
