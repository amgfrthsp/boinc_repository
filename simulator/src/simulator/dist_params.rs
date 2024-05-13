// Distributions for all random hosts

use rand::{prelude::Distribution, Rng};
use rand_distr::{LogNormal, Weibull};

pub enum DistributionConfig {
    Weibull { scale: f64, shape: f64 },
    LogNormal { mu: f64, sigma: f64 },
}

pub enum SimulationDistribution {
    Weibull { distr: Weibull<f64> },
    LogNormal { distr: LogNormal<f64> },
}

impl SimulationDistribution {
    pub fn new(config: DistributionConfig) -> Self {
        match config {
            DistributionConfig::LogNormal { mu, sigma } => Self::LogNormal {
                distr: LogNormal::new(mu, sigma).unwrap(),
            },
            DistributionConfig::Weibull { scale, shape } => Self::Weibull {
                distr: Weibull::new(scale, shape).unwrap(),
            },
        }
    }
}

impl Distribution<f64> for SimulationDistribution {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> f64 {
        match self {
            Self::Weibull { distr } => distr.sample(rng),
            Self::LogNormal { distr } => distr.sample(rng),
        }
    }
}

pub struct ClientAvailabilityParams {
    pub availability: DistributionConfig,
    pub unavailability: DistributionConfig,
}

pub const ALL_RANDOM_HOSTS: ClientAvailabilityParams = ClientAvailabilityParams {
    availability: DistributionConfig::Weibull {
        scale: 2.964,
        shape: 0.393,
    },
    unavailability: DistributionConfig::LogNormal {
        mu: -0.586,
        sigma: 2.844,
    },
};
