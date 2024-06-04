use dslab_core::Id;
use serde::Serialize;

use dslab_compute::multicore::CoresDependency;

#[derive(Serialize, Debug, Clone, Default)]
pub struct InputFileMetadata {
    pub workunit_id: WorkunitId,
    pub size: u64,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct OutputFileMetadata {
    pub result_id: ResultId,
    pub size: u64,
}

#[derive(Serialize, Debug, Clone)]
pub enum DataServerFile {
    Input(InputFileMetadata),
    Output(OutputFileMetadata),
}

impl DataServerFile {
    pub fn id(&self) -> u64 {
        match self {
            DataServerFile::Input(InputFileMetadata { workunit_id, .. }) => *workunit_id,
            DataServerFile::Output(OutputFileMetadata { result_id, .. }) => *result_id,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            DataServerFile::Input(InputFileMetadata { size, .. }) => *size,
            DataServerFile::Output(OutputFileMetadata { size, .. }) => *size,
        }
    }
}

pub type JobSpecId = u64;

#[derive(Serialize, Debug, Clone)]
pub struct JobSpec {
    pub id: JobSpecId,
    pub gflops: f64,
    pub memory: u64,
    pub cores: u32,
    pub cores_dependency: CoresDependency,
    pub delay_bound: f64,
    pub min_quorum: u64,
    pub target_nresults: u64,
    pub input_file: InputFileMetadata,
    pub output_file: OutputFileMetadata,
}

#[derive(Serialize, Debug, Clone)]
pub struct ResultRequest {
    pub spec: JobSpec,
    pub report_deadline: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResultState {
    Unsent,
    InProgress,
    Over,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResultOutcome {
    Undefined,
    Success,
    NoReply,
    DidntNeed,
    ValidateError,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ValidateState {
    Init,
    Valid,
    Invalid,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum AssimilateState {
    Init,
    Ready,
    Done,
}

#[derive(Debug, Clone, PartialEq, Copy)]
#[allow(dead_code)]
pub enum FileDeleteState {
    Init,
    Ready,
    Done,
}

pub type WorkunitId = u64;

#[derive(Debug, Clone)]
pub struct WorkunitInfo {
    pub id: WorkunitId,
    pub spec: JobSpec,
    pub result_ids: Vec<ResultId>,
    pub client_ids: Vec<Id>,
    pub transition_time: f64,
    pub need_validate: bool,
    pub file_delete_state: FileDeleteState,
    pub canonical_resultid: Option<ResultId>,
    pub assimilate_state: AssimilateState,
}

pub type ResultId = u64;

#[derive(Debug, Clone)]
pub struct ResultInfo {
    pub id: ResultId,
    pub workunit_id: WorkunitId,
    pub report_deadline: f64,
    pub server_state: ResultState,
    pub outcome: ResultOutcome,
    pub validate_state: ValidateState,
    pub file_delete_state: FileDeleteState,
    pub in_shared_mem: bool,
    pub time_sent: f64,
    pub client_id: Id,
    pub is_correct: bool,
    pub claimed_credit: f64,
}
