use crate::server::job::JobRequest;

pub type TaskRequest = JobRequest;

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
    pub(crate) req: JobRequest,
    pub(crate) state: TaskState,
}
