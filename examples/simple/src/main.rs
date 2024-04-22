use boinc_simulator::{config::sim_config::SimulationConfig, simulator::simulator::Simulator};
use env_logger::Builder;
use std::io::Write;

fn init_logger() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn simulation(sim_config: SimulationConfig) {
    let seed = 123;

    let mut simulator = Simulator::new(seed, sim_config);
    // run until completion
    simulator.run();
}

fn main() {
    init_logger();
    let config = SimulationConfig::from_file("config.yaml");
    simulation(config);
}
