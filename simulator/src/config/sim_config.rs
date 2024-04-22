//! Simulation configuration.

use serde::{Deserialize, Serialize};

/// Holds raw simulation config parsed from YAML file.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
struct RawSimulationConfig {
    pub network_latency: Option<f64>,
    pub network_bandwidth: Option<f64>,
    pub use_shared_network: Option<bool>,
    pub job_generator: Option<JobGeneratorConfig>,
    pub server: Option<ServerConfig>,
    pub hosts: Option<Vec<HostConfig>>,
}

/// Holds configuration of a single client or a set of identical clients.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct HostConfig {
    pub cpus: u32,
    pub memory: u64,
    pub disk_capacity: u64,
    pub disk_read_bandwidth: f64,
    pub disk_write_bandwidth: f64,
    pub local_latency: f64,
    pub local_bandwidth: f64,
    pub count: Option<u32>,
}

/// Holds configuration of a job generator
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct JobGeneratorConfig {
    pub local_latency: f64,
    pub local_bandwidth: f64,
    pub job_count: u64,
}

/// Holds configuration of the main server
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct ServerConfig {
    pub local_latency: f64,
    pub local_bandwidth: f64,
    pub data_servers: Vec<DataServerConfig>,
}

/// Holds configuration of a single data server or a set of identical data servers.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DataServerConfig {
    pub disk_capacity: u64,
    pub disk_read_bandwidth: f64,
    pub disk_write_bandwidth: f64,
    /// Number of data server instances.
    pub count: Option<u32>,
}

/// Represents simulation configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SimulationConfig {
    pub network_latency: f64,
    pub network_bandwidth: f64,
    pub use_shared_network: bool,
    pub hosts: Vec<HostConfig>,
    pub job_generator: JobGeneratorConfig,
    pub server: ServerConfig,
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

        Self {
            network_latency: raw.network_latency.unwrap_or(0.5),
            network_bandwidth: raw.network_bandwidth.unwrap_or(1000.),
            use_shared_network: raw.use_shared_network.unwrap_or(false),
            hosts: raw.hosts.unwrap_or_default(),
            job_generator: raw.job_generator.unwrap_or_default(),
            server: raw.server.unwrap_or_default(),
        }
    }
}
