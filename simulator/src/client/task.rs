use crate::server::job::{JobSpec, OutputFileMetadata, ResultRequest};

pub type TaskSpec = JobSpec;
pub type TaskRequest = ResultRequest;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum TaskState {
    New,
    Assigned,
    Downloading,
    Reading,
    Running,
    Writing,
    Uploading,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub(crate) spec: TaskSpec,
    pub(crate) output_file: OutputFileMetadata,
    pub(crate) state: TaskState,
}
