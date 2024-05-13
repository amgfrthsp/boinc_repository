use dslab_compute::multicore::Compute;
use dslab_core::context::SimulationContext;
use dslab_core::Simulation;
use dslab_core::{component::Id, log_info};
use dslab_network::models::TopologyAwareNetworkModel;
use dslab_network::Link;
use dslab_network::{models::SharedBandwidthNetworkModel, Network};
use dslab_storage::disk::DiskBuilder;
use serde::Serialize;
use std::rc::Rc;
use std::{cell::RefCell, time::Instant};
use sugars::{rc, refcell};

use crate::client::rr_simulation::RRSimulation;
use crate::client::scheduler::Scheduler as ClientScheduler;
use crate::client::storage::FileStorage;
use crate::client::utils::Utilities;
use crate::config::sim_config::{ClientConfig, ServerConfig, SimulationConfig};
use crate::server::db_purger::DBPurger;
use crate::server::feeder::{Feeder, SharedMemoryItem, SharedMemoryItemState};
use crate::server::file_deleter::FileDeleter;
use crate::server::job::ResultId;
use crate::server::stats::ServerStats;
use crate::{
    client::client::Client,
    server::{
        assimilator::Assimilator, data_server::DataServer, database::BoincDatabase,
        job_generator::JobGenerator, scheduler::Scheduler as ServerScheduler, server::Server,
        transitioner::Transitioner, validator::Validator,
    },
};

use super::dist_params::{SimulationDistribution, ALL_RANDOM_HOSTS};

#[derive(Clone, Serialize)]
pub struct StartServer {
    pub finish_time: f64,
}

#[derive(Clone, Serialize)]
pub struct StartClient {
    pub server_id: Id,
    pub data_server_id: Id,
    pub finish_time: f64,
}

pub struct Simulator {
    sim: Rc<RefCell<Simulation>>,
    network: Rc<RefCell<Network>>,
    hosts: Vec<String>,
    server_id: Option<Id>,
    server_stats: Option<Rc<RefCell<ServerStats>>>,
    data_server_id: Option<Id>,
    client_ids: Vec<Id>,
    ctx: SimulationContext,
    sim_config: SimulationConfig,
}

impl Simulator {
    pub fn new(sim_config: SimulationConfig) -> Self {
        let mut sim = Simulation::new(sim_config.seed);

        let network = rc!(refcell!(Network::new(
            Box::new(TopologyAwareNetworkModel::new()),
            sim.create_context("net"),
        )));

        // context for starting server and clients
        let ctx = sim.create_context("ctx");

        let mut sim = Self {
            sim: rc!(refcell!(sim)),
            network: network.clone(),
            hosts: Vec::new(),
            server_id: None,
            server_stats: None,
            data_server_id: None,
            client_ids: Vec::new(),
            ctx,
            sim_config,
        };

        let server_node_name = sim.add_server(sim.sim_config.server.clone());
        // Add hosts from config
        for host_config in sim.sim_config.clients.clone() {
            let count = host_config.count.unwrap_or(1);
            for _ in 0..count {
                sim.add_host(host_config.clone(), &server_node_name);
            }
        }

        sim.network.borrow_mut().init_topology();
        sim.sim.borrow_mut().add_handler("net", network.clone());

        sim
    }

    pub fn run(&mut self) {
        if self.server_id.is_none() {
            println!("Server is not added");
            return;
        }
        log_info!(self.ctx, "Simulation started");
        self.ctx.emit_now(
            StartServer {
                finish_time: self.sim_config.sim_duration * 3600.,
            },
            self.server_id.unwrap(),
        );
        for client_id in &self.client_ids {
            self.ctx.emit_now(
                StartClient {
                    server_id: self.server_id.unwrap(),
                    data_server_id: self.data_server_id.unwrap(),
                    finish_time: self.sim_config.sim_duration * 3600.,
                },
                *client_id,
            );
        }

        let t = Instant::now();
        self.sim.borrow_mut().step_until_no_events();
        let duration = t.elapsed().as_secs_f64();

        log_info!(self.ctx, "Simulation finished");

        let stats_ref = self.server_stats.clone().unwrap();
        let stats = stats_ref.borrow();

        println!("Jobs processed: {}", stats.n_workunits_total);
        println!("Results processed: {}", stats.n_results_total);
        println!("Calculated {} FLOPS", stats.flops_total);
        println!(
            "Average result processing time: {:.2}",
            stats.results_processing_time / stats.n_results_total as f64
        );
        println!(
            "Min result processing time: {:.2}",
            stats.min_result_processing_time
        );
        println!(
            "Max result processing time: {:.2}",
            stats.max_result_processing_time
        );
        println!(
            "Results missed deadline: {}, {:.2}%",
            stats.n_miss_deadline,
            stats.n_miss_deadline as f64 / stats.n_results_total as f64 * 100.
        );
        println!(
            "Valid results: {:.2}%",
            stats.n_results_valid as f64 / stats.n_results_total as f64 * 100.
        );
        println!(
            "Invalid results: {:.2}%",
            stats.n_results_invalid as f64 / stats.n_results_total as f64 * 100.
        );
        println!("Total credit granted: {:.8}", stats.total_credit_granted);
        println!("Elapsed time: {:.2}s", duration);
        //println!("Scheduling time: {:.2}s", server.borrow().scheduling_time);
        println!(
            "Simulation speedup: {:.2}",
            self.sim.borrow_mut().time() / duration
        );
        let event_count = self.sim.borrow_mut().event_count();
        println!(
            "Processed {} events in {:.2?}s ({:.0} events/s)",
            event_count,
            duration,
            event_count as f64 / duration
        );
    }

    pub fn add_server(&mut self, config: ServerConfig) -> String {
        let n = self.hosts.len();
        let node_name = &format!("host{}", n);
        self.network.borrow_mut().add_node(
            node_name,
            Box::new(SharedBandwidthNetworkModel::new(
                config.local_bandwidth,
                config.local_latency,
            )),
        );
        self.hosts.push(node_name.to_string());
        let server_name = &format!("{}::server", node_name);

        let stats = rc!(refcell!(ServerStats::new()));

        // Database
        let database = rc!(BoincDatabase::new());

        // Job generator
        let job_generator_name = &format!("{}::job_generator", node_name);
        let job_generator = JobGenerator::new(
            self.sim.borrow_mut().create_context(job_generator_name),
            config.job_generator.clone(),
        );

        // Adding daemon components
        // Validator
        let validator_name = &format!("{}::validator", server_name);
        let validator = Validator::new(
            database.clone(),
            self.sim.borrow_mut().create_context(validator_name),
            config.validator.clone(),
            stats.clone(),
        );

        // Assimilator
        let assimilator_name = &format!("{}::assimilator", server_name);
        let assimilator = Assimilator::new(
            database.clone(),
            self.sim.borrow_mut().create_context(assimilator_name),
            config.assimilator.clone(),
            stats.clone(),
        );

        // Transitioner
        let transitioner_name = &format!("{}::transitioner", server_name);
        let transitioner = Transitioner::new(
            database.clone(),
            self.sim.borrow_mut().create_context(transitioner_name),
            config.transitioner.clone(),
            stats.clone(),
        );

        // Database purger
        let db_purger_name = &format!("{}::db_purger", server_name);
        let db_purger = DBPurger::new(
            database.clone(),
            self.sim.borrow_mut().create_context(db_purger_name),
            config.db_purger.clone(),
        );

        // Feeder
        let empty_slot = SharedMemoryItem {
            state: SharedMemoryItemState::Empty,
            result_id: 0,
        };
        let shared_memory: Rc<RefCell<Vec<ResultId>>> = rc!(refcell!(Vec::new()));

        let feeder_name = &format!("{}::feeder", server_name);
        let feeder: Feeder = Feeder::new(
            shared_memory.clone(),
            database.clone(),
            self.sim.borrow_mut().create_context(feeder_name),
            config.feeder.clone(),
        );

        // Scheduler
        let scheduler_name = &format!("{}::scheduler", server_name);
        let scheduler = rc!(refcell!(ServerScheduler::new(
            self.network.clone(),
            database.clone(),
            shared_memory.clone(),
            self.sim.borrow_mut().create_context(scheduler_name),
            config.scheduler.clone(),
        )));
        let scheduler_id = self
            .sim
            .borrow_mut()
            .add_handler(scheduler_name, scheduler.clone());
        self.network
            .borrow_mut()
            .set_location(scheduler_id, node_name);

        // Data server
        let data_server_name = &format!("{}::data_server", server_name);
        // file storage
        let disk_name = format!("{}::disk", data_server_name);
        let disk = rc!(refcell!(DiskBuilder::simple(
            config.data_server.disk_capacity,
            config.data_server.disk_read_bandwidth,
            config.data_server.disk_write_bandwidth
        )
        .build(self.sim.borrow_mut().create_context(&disk_name))));
        self.sim.borrow_mut().add_handler(disk_name, disk.clone());
        let data_server: Rc<RefCell<DataServer>> = rc!(refcell!(DataServer::new(
            self.network.clone(),
            disk,
            self.sim.borrow_mut().create_context(data_server_name),
            config.data_server.clone(),
            stats.clone()
        )));
        let data_server_id = self
            .sim
            .borrow_mut()
            .add_handler(data_server_name, data_server.clone());
        self.data_server_id = Some(data_server_id);
        self.network
            .borrow_mut()
            .set_location(data_server_id, node_name);

        // File deleter
        let file_deleter_name = &format!("{}::file_deleter", server_name);
        let file_deleter = FileDeleter::new(
            database.clone(),
            data_server.clone(),
            self.sim.borrow_mut().create_context(file_deleter_name),
            config.file_deleter.clone(),
        );

        let server = rc!(refcell!(Server::new(
            database.clone(),
            job_generator,
            validator,
            assimilator,
            transitioner,
            feeder,
            file_deleter,
            db_purger,
            scheduler,
            data_server,
            self.sim.borrow_mut().create_context(server_name),
            config,
            stats.clone()
        )));
        self.server_stats = Some(stats.clone());
        let server_id = self
            .sim
            .borrow_mut()
            .add_handler(server_name, server.clone());
        self.server_id = Some(server_id);
        self.network.borrow_mut().set_location(server_id, node_name);

        server.borrow_mut().generate_jobs();

        node_name.clone()
    }

    pub fn add_host(&mut self, config: ClientConfig, server_node_name: &str) {
        let n = self.hosts.len();
        let node_name = &format!("host{}", n);
        self.network.borrow_mut().add_node(
            node_name,
            Box::new(SharedBandwidthNetworkModel::new(
                config.local_bandwidth,
                config.local_latency,
            )),
        );
        self.network.borrow_mut().add_link(
            &node_name,
            server_node_name,
            Link::shared(config.network_bandwidth, config.network_latency),
        );
        self.hosts.push(node_name.to_string());
        // compute
        let compute_name = format!("{}::compute", node_name);
        let compute = rc!(refcell!(Compute::new(
            config.speed,
            config.cpus,
            config.memory * 1000,
            self.sim.borrow_mut().create_context(&compute_name),
        )));
        self.sim
            .borrow_mut()
            .add_handler(compute_name, compute.clone());
        // disk
        let disk_name = format!("{}::disk", node_name);
        let disk = rc!(refcell!(DiskBuilder::simple(
            config.disk_capacity,
            config.disk_read_bandwidth,
            config.disk_write_bandwidth
        )
        .build(self.sim.borrow_mut().create_context(&disk_name))));
        self.sim.borrow_mut().add_handler(disk_name, disk.clone());

        let client_name = &format!("{}::client", node_name);

        // File Storage
        let file_storage: Rc<FileStorage> = rc!(FileStorage::new());

        let utilities_name = &format!("{}::utilities", client_name);
        let utilities = rc!(refcell!(Utilities::new(
            compute.clone(),
            file_storage.clone(),
            self.sim.borrow_mut().create_context(utilities_name)
        )));

        // RR Simulator
        let rr_simulator_name = &format!("{}::rr_simulator", client_name);
        let rr_simulator = rc!(refcell!(RRSimulation::new(
            config.buffered_work_lower_bound,
            config.buffered_work_upper_bound,
            file_storage.clone(),
            compute.clone(),
            utilities.clone(),
            self.sim.borrow_mut().create_context(rr_simulator_name),
        )));

        // Scheduler
        let scheduler_name = &format!("{}::scheduler", client_name);
        let scheduler = ClientScheduler::new(
            rr_simulator.clone(),
            compute.clone(),
            file_storage.clone(),
            utilities.clone(),
            self.sim.borrow_mut().create_context(scheduler_name),
        );

        let client = Client::new(
            compute,
            disk,
            self.network.clone(),
            utilities.clone(),
            scheduler,
            rr_simulator.clone(),
            file_storage.clone(),
            self.sim.borrow_mut().create_context(client_name),
            config,
            // Find better distribution for reliability
            self.ctx.gen_range(0.8..1.),
            SimulationDistribution::new(ALL_RANDOM_HOSTS.availability),
            SimulationDistribution::new(ALL_RANDOM_HOSTS.unavailability),
        );
        let client_id = self
            .sim
            .borrow_mut()
            .add_handler(client_name, rc!(refcell!(client)));
        self.network.borrow_mut().set_location(client_id, node_name);
        self.client_ids.push(client_id);
    }
}
