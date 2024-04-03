use crate::server::job::{JobSpec, OutputFileMetadata};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ClientResultState {
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
pub struct ResultInfo {
    pub spec: JobSpec,
    pub output_file: OutputFileMetadata,
    pub state: ClientResultState,
}
