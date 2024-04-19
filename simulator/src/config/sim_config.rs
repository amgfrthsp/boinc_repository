//! Simulation configuration.

use serde::{Deserialize, Serialize};

/// Holds raw simulation config parsed from YAML file.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
struct RawSimulationConfig {}

/// Holds configuration of a single physical host or a set of identical hosts.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct HostConfig {
    /// Host name.
    /// Should be set if count = 1.
    pub name: Option<String>,
    /// Host name prefix.
    /// Full name is produced by appending host instance number to the prefix.
    /// Should be set if count > 1.
    pub name_prefix: Option<String>,
    /// Host CPU capacity.
    pub cpus: u32,
    /// Host memory capacity in GB.
    pub memory: u64,
    /// Number of such hosts.
    pub count: Option<u32>,
}

/// Represents simulation configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SimulationConfig {
    /// Period length in seconds for sending statistics from host to monitoring.
    pub send_stats_period: f64,
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
            send_stats_period: raw.send_stats_period.unwrap_or(0.5),
        }
    }
}
