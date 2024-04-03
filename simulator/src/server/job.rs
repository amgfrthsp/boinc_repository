use serde::Serialize;

use dslab_compute::multicore::CoresDependency;

pub type FileId = u64;

#[derive(Serialize, Debug, Clone)]
pub struct InputFileMetadata {
    pub id: FileId,
    pub workunit_id: WorkunitId,
    pub size: u64,
}

#[derive(Serialize, Debug, Clone)]
pub struct OutputFileMetadata {
    pub id: FileId,
    pub result_id: ResultId,
    pub size: u64,
}

pub type JobSpecId = u64;

#[derive(Serialize, Debug, Clone)]
pub struct JobSpec {
    pub id: JobSpecId,
    pub flops: f64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
    pub input_file: InputFileMetadata,
}

#[derive(Serialize, Debug, Clone)]
pub struct ResultRequest {
    pub spec: JobSpec,
    pub output_file: OutputFileMetadata,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ResultState {
    Inactive,
    Unsent,
    InProgress,
    Over,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ResultOutcome {
    Undefined,
    Success,
    CouldntSend,
    ClientError,
    NoReply,
    DidntNeed,
    ValidateError,
    ClientDetached,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ValidateState {
    Undefined,
    Init,
    Valid,
    Invalid,
    NoCheck,
    Error,
    Inconclusive,
    TooLate,
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
    pub(crate) id: WorkunitId,
    pub(crate) spec: JobSpec,
    pub(crate) result_ids: Vec<ResultId>,
    pub(crate) transition_time: f64,
    pub(crate) delay_bound: f64,
    pub(crate) min_quorum: u64,
    pub(crate) target_nresults: u64,
    pub(crate) need_validate: bool,
    pub(crate) file_delete_state: FileDeleteState,
    pub(crate) canonical_resultid: Option<ResultId>,
    pub(crate) assimilate_state: AssimilateState,
}

pub type ResultId = u64;

#[derive(Debug, Clone)]
pub struct ResultInfo {
    pub(crate) id: ResultId,
    pub(crate) workunit_id: WorkunitId,
    pub(crate) report_deadline: f64,
    pub(crate) server_state: ResultState,
    pub(crate) outcome: ResultOutcome,
    pub(crate) validate_state: ValidateState,
    pub(crate) file_delete_state: FileDeleteState,
    pub(crate) in_shared_mem: bool,
}
