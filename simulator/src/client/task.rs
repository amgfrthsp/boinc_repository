use dslab_core::EventId;

use crate::server::job::{JobSpec, OutputFileMetadata};

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ResultState {
    Downloading,
    ReadyToExecute,
    Running,
    Preempted { comp_id: EventId },
    ReadyToUpload,
    ReadyToNotify,
    Over,
}

#[derive(Debug, Clone)]
pub struct ResultInfo {
    pub spec: JobSpec,
    pub report_deadline: f64,
    pub output_file: OutputFileMetadata,
    pub state: ResultState,
    pub comp_id: Option<EventId>,
}
