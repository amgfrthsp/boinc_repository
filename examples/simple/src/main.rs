use boinc_simulator::{config::sim_config::SimulationConfig, simulator::simulator::Simulator};
use env_logger::Builder;
use std::io::Write;

use rand::Rng;

fn init_logger() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn simulation(mut sim_config: SimulationConfig) {
    let mut rng = rand::thread_rng();
    let cnts = vec![1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000];
    for cnt in cnts {
        sim_config.clients[0].count = Some(cnt);
        for _ in 0..5 {
            sim_config.seed = rng.gen_range(0..1000);
            println!("seed: {}", sim_config.seed);
            let mut simulator = Simulator::new(sim_config.clone());
            // run until completion
            simulator.run();
        }
    }
}

fn main() {
    init_logger();
    let config = SimulationConfig::from_file("config.yaml");
    simulation(config);
}
