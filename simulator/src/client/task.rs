use dslab_core::EventId;

use crate::server::job::JobSpec;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ResultState {
    Downloading,
    Unstarted,
    Reading,
    Canceled,
    Running,
    Preempted { comp_id: EventId },
    Writing,
    Uploading,
    Notifying,
    Over,
    Deleted,
}

#[derive(Debug, Clone)]
pub struct ResultInfo {
    pub spec: JobSpec,
    pub report_deadline: f64,
    pub state: ResultState,
    pub time_added: f64,
    pub comp_id: Option<EventId>,
    pub sim_miss_deadline: bool,
}
