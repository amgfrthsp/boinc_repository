use crate::server::job::{JobSpec, OutputFileMetadata};

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ResultState {
    Downloading,
    ReadyToExecute,
    Executing,
    ReadyToUpload,
    ReadyToNotify,
    Over,
}

#[derive(Debug, Clone)]
pub struct ResultInfo {
    pub spec: JobSpec,
    pub output_file: OutputFileMetadata,
    pub state: ResultState,
}
