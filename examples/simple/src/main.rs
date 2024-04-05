use boinc_simulator::simulator::simulator::Simulator;
use clap::Parser;
use env_logger::Builder;
use std::io::Write;

/// Server-clients example
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of hosts
    #[clap(long, default_value_t = 100)]
    host_count: usize,

    /// Number of jobs
    #[clap(long, default_value_t = 100000)]
    job_count: u64,

    /// Use shared network
    #[clap(long)]
    use_shared_network: bool,
}

fn main() {
    // logger
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // params
    let args = Args::parse();
    let host_count = 2;
    let local_latency = 0.0;
    let local_bandwidth = 10000;
    let network_latency = 0.5;
    let network_bandwidth = 1000;
    let host_disk_capacity = 5000;
    let data_server_disk_capacity = 25000;
    let disk_read_bandwidth = 2000.;
    let disk_write_bandwidth = 2000.;
    let use_shared_network = args.use_shared_network;
    let seed = 123;

    let mut simulator = Simulator::new(
        seed,
        use_shared_network,
        network_bandwidth as f64,
        network_latency,
    );

    simulator.add_job_generator(local_bandwidth as f64, local_latency);
    simulator.add_server(
        local_bandwidth as f64,
        local_latency,
        host_disk_capacity,
        disk_read_bandwidth,
        disk_write_bandwidth,
    );
    for _ in 0..host_count {
        simulator.add_host(
            local_bandwidth as f64,
            local_latency,
            data_server_disk_capacity,
            disk_read_bandwidth,
            disk_write_bandwidth,
        );
    }

    // run until completion
    simulator.run();
}
