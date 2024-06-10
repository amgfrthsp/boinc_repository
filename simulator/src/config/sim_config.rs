//! Simulation configuration.

use serde::{Deserialize, Serialize};

use crate::{common::HOUR, simulator::dist_params::DistributionConfig};

/// Holds raw simulation config parsed from YAML file.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
struct RawSimulationConfig {
    pub seed: Option<u64>,
    pub sim_duration: Option<f64>,
    pub projects: Option<Vec<ProjectConfig>>,
    pub clients: Option<Vec<ClientGroupConfig>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ClientCpuPower {
    // number of cores
    pub cores: u32,
    // core speed (GFLOPs/core)
    pub speed: f64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ClientGroupConfig {
    // trace file with CPU power of hosts
    pub trace: Option<String>,
    // if trace is None, client group size
    pub count: Option<u32>,
    // if trace is None, CPU power of hosts
    pub cpu: Option<ClientCpuPower>,
    // work fetch interval (s)
    pub work_fetch_interval: f64,
    // min queued jobs runtime (s)
    pub buffered_work_min: f64,
    // max queued jobs runtime (s)
    pub buffered_work_max: f64,
    // memory (GB)
    pub memory: u64,
    // disk capacity (GB)
    pub disk_capacity: u64,
    // disk read bandwidth (MB/s)
    pub disk_read_bandwidth: f64,
    // disk write bandwidth (MB/s)
    pub disk_write_bandwidth: f64,
    // network latency (ms)
    pub network_latency: f64,
    // network bandwidth (MB/s)
    pub network_bandwidth: f64,
    // latency within a machine (ms)
    pub local_latency: f64,
    // bandwidth within a machine (MB/s)
    pub local_bandwidth: f64,
    // client's statistics report interval (s)
    pub report_status_interval: f64,
    pub reliability_distribution: Option<DistributionConfig>,
    pub availability_distribution: Option<DistributionConfig>,
    pub unavailability_distribution: Option<DistributionConfig>,
}

impl ClientGroupConfig {
    pub fn from_h_to_sec(&mut self) {
        self.work_fetch_interval *= HOUR;
        self.buffered_work_min *= HOUR;
        self.buffered_work_max *= HOUR;
        self.report_status_interval *= HOUR;
    }
}

/// Holds configuration of a project
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct ProjectConfig {
    pub name: String,
    pub server: ServerConfig,
}

/// Holds configuration of the main server
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct ServerConfig {
    // latency within a machine (ms)
    pub local_latency: f64,
    // bandwidth within a machine (MB/s)
    pub local_bandwidth: f64,
    // server's statistics report interval (s)
    pub report_status_interval: f64,
    pub job_generator: JobGeneratorConfig,
    pub data_server: DataServerConfig,
    pub assimilator: AssimilatorConfig,
    pub validator: ValidatorConfig,
    pub transitioner: TransitionerConfig,
    pub db_purger: DBPurgerConfig,
    pub file_deleter: FileDeleterConfig,
    pub feeder: FeederConfig,
    pub scheduler: SchedulerConfig,
}

impl ServerConfig {
    pub fn from_h_to_sec(&mut self) {
        self.assimilator.interval *= HOUR;
        self.feeder.interval *= HOUR;
        self.validator.interval *= HOUR;
        self.file_deleter.interval *= HOUR;
        self.db_purger.interval *= HOUR;
        self.transitioner.interval *= HOUR;

        self.report_status_interval *= HOUR;
        self.job_generator.delay_min *= HOUR;
        self.job_generator.delay_max *= HOUR;
    }
}

/// Holds configuration of a job generator
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct JobGeneratorConfig {
    // wu size in [gflops_min; gflops_max] (GFLOPs)
    pub gflops_min: f64,
    pub gflops_max: f64,
    // wu memory size in memory_min; memory_max] (MB)
    pub memory_min: u64,
    pub memory_max: u64,
    // wu cores amount in [cores_min; cores_max]
    pub cores_min: u32,
    pub cores_max: u32,
    // wu delay time in [delay_min; delay_max] (s)
    pub delay_min: f64,
    pub delay_max: f64,
    // wu min_quorum value in [min_quorum_min; min_quorum_max]
    pub min_quorum_min: u64,
    pub min_quorum_max: u64,
    // initial number of results, target_nresults >= min_quorum
    pub target_nresults_min: u64,
    pub target_nresults_max: u64,
    // wu input size in [input_size_min; input_size_max] (MB)
    pub input_size_min: u64,
    pub input_size_max: u64,
    // wu output size in [output_size_min; output_size_max] (MB)
    pub output_size_min: u64,
    pub output_size_max: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct AssimilatorConfig {
    // invokation interval
    pub interval: f64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct ValidatorConfig {
    // invokation interval (s)
    pub interval: f64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct TransitionerConfig {
    // invokation interval (s)
    pub interval: f64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct DBPurgerConfig {
    // invokation interval (s)
    pub interval: f64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct FileDeleterConfig {
    // invokation interval (s)
    pub interval: f64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct FeederConfig {
    // results shared memory size (units)
    pub shared_memory_size: usize,
    // invokation interval (s)
    pub interval: f64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct SchedulerConfig {
    pub est_runtime_error_distribution: Option<DistributionConfig>,
}

/// Holds configuration of a single data server or a set of identical data servers.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct DataServerConfig {
    // disk capacity (GB)
    pub disk_capacity: u64,
    // disk read bandwidth (MB/s)
    pub disk_read_bandwidth: f64,
    // disk write bandwidth (MB/s)
    pub disk_write_bandwidth: f64,
}

/// Represents simulation configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SimulationConfig {
    pub seed: u64,
    // Simulation duration in hours
    pub sim_duration: f64,
    pub clients: Vec<ClientGroupConfig>,
    pub projects: Vec<ProjectConfig>,
}

impl SimulationConfig {
    /// Creates simulation config by reading parameter values from YAM file
    /// (uses default values if some parameters are absent).
    pub fn from_file(file_name: &str) -> Self {
        let raw: RawSimulationConfig = serde_yaml::from_str(
            &std::fs::read_to_string(file_name)
                .unwrap_or_else(|_| panic!("Can't read file {}", file_name)),
        )
        .unwrap_or_else(|_| panic!("Can't parse YAML from file {}", file_name));

        let mut config = Self {
            seed: raw.seed.unwrap_or(124),
            sim_duration: raw.sim_duration.unwrap_or(24.),
            clients: raw.clients.unwrap_or_default(),
            projects: raw.projects.unwrap_or_default(),
        };

        for client_group in config.clients.iter_mut() {
            client_group.from_h_to_sec();
        }
        for project in config.projects.iter_mut() {
            project.server.from_h_to_sec();
        }

        config
    }
}
