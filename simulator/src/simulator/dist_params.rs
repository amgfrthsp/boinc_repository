// Distributions for all random hosts

use rand::{prelude::Distribution, Rng};
use rand_distr::{LogNormal, Normal, Uniform, Weibull};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DistributionConfig {
    Weibull { scale: f64, shape: f64 },
    LogNormal { mu: f64, sigma: f64 },
    Uniform { min: f64, max: f64 },
    Normal { mu: f64, sigma: f64 },
}

pub enum SimulationDistribution {
    Weibull { distr: Weibull<f64> },
    LogNormal { distr: LogNormal<f64> },
    Uniform { distr: Uniform<f64> },
    Normal { distr: Normal<f64> },
}

impl SimulationDistribution {
    pub fn new(config: DistributionConfig) -> Self {
        match config {
            DistributionConfig::Normal { mu, sigma } => Self::Normal {
                distr: Normal::new(mu, sigma).unwrap(),
            },
            DistributionConfig::LogNormal { mu, sigma } => Self::LogNormal {
                distr: LogNormal::new(mu, sigma).unwrap(),
            },
            DistributionConfig::Weibull { scale, shape } => Self::Weibull {
                distr: Weibull::new(scale, shape).unwrap(),
            },
            DistributionConfig::Uniform { min, max } => Self::Uniform {
                distr: Uniform::new(min, max),
            },
        }
    }
}

impl Distribution<f64> for SimulationDistribution {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> f64 {
        match self {
            Self::Weibull { distr } => distr.sample(rng),
            Self::LogNormal { distr } => distr.sample(rng),
            Self::Uniform { distr } => distr.sample(rng),
            Self::Normal { distr } => distr.sample(rng),
        }
    }
}

pub struct ClientAvailabilityParams {
    pub availability: DistributionConfig,
    pub unavailability: DistributionConfig,
}

pub const CLIENT_AVAILABILITY_ALL_RANDOM_HOSTS: ClientAvailabilityParams =
    ClientAvailabilityParams {
        availability: DistributionConfig::Weibull {
            scale: 2.964,
            shape: 0.393,
        },
        unavailability: DistributionConfig::LogNormal {
            mu: -0.586,
            sigma: 2.844,
        },
    };

pub const SCHEDULER_EST_RUNTIME_ERROR: DistributionConfig =
    DistributionConfig::Normal { mu: 1., sigma: 0. };
