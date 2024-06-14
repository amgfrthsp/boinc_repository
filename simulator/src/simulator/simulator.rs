use csv::ReaderBuilder;
use dslab_compute::multicore::Compute;
use dslab_core::context::SimulationContext;
use dslab_core::{log_info, Event, EventHandler, Simulation};
use dslab_network::{models::SharedBandwidthNetworkModel, Network};
use dslab_storage::disk::DiskBuilder;
use serde::Serialize;
use std::borrow::Borrow;
use std::cell::{Ref, RefMut};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::rc::Rc;
use std::{cell::RefCell, time::Instant};
use sugars::{rc, refcell};

use crate::client::client::ProjectInfo;
use crate::client::rr_simulation::RRSimulation;
use crate::client::stats::ClientStats;
use crate::client::storage::FileStorage;
use crate::client::utils::Utilities;
use crate::config::sim_config::{
    ClientCpuPower, ClientGroupConfig, ProjectConfig, SimulationConfig,
};
use crate::project::db_purger::DBPurger;
use crate::project::feeder::Feeder;
use crate::project::file_deleter::FileDeleter;
use crate::project::job::ResultId;
use crate::project::project::BoincProject;
use crate::project::server::{GenerateJobs, JobsGenerationCompleted};
use crate::project::stats::ServerStats;
use crate::simulator::print_stats::{print_clients_stats, print_project_stats};
use crate::{
    client::client::Client,
    project::{
        assimilator::Assimilator, data_server::DataServer, database::BoincDatabase,
        job_generator::JobGenerator, scheduler::Scheduler as ServerScheduler,
        server::ProjectServer, transitioner::Transitioner, validator::Validator,
    },
};

use super::dist_params::{
    DistributionConfig, SimulationDistribution, CLIENT_AVAILABILITY_ALL_RANDOM_HOSTS,
    SCHEDULER_EST_RUNTIME_ERROR,
};

#[derive(Clone, Serialize)]
pub struct StartServer {
    pub finish_time: f64,
}

#[derive(Clone, Serialize)]
pub struct StartClient {
    pub finish_time: f64,
}

pub struct Simulator {
    simulation: RefCell<Simulation>,
    network: Rc<RefCell<Network>>,
    projects: HashMap<String, BoincProject>, // name -> project
    clients: Vec<Rc<Client>>,
    ctx: SimulationContext,
    sim_config: SimulationConfig,
}

impl Simulator {
    pub fn new(sim_config: SimulationConfig) -> Self {
        let mut simulation = Simulation::new(sim_config.seed);

        // context for starting server and clients
        let ctx = simulation.create_context("ctx");
        simulation.add_handler("ctx", rc!(refcell!(EmptyEventHandler {})));

        let network_name = "network";
        let network = rc!(refcell!(Network::new(
            Box::new(SharedBandwidthNetworkModel::new(70., 40. / 1000.)),
            simulation.create_context(network_name),
        )));

        simulation.add_handler(network_name, network.clone());

        let mut simulator = Self {
            simulation: refcell!(simulation),
            network,
            projects: HashMap::new(),
            clients: Vec::new(),
            ctx,
            sim_config,
        };

        for project_config in simulator.sim_config.projects.clone() {
            simulator.add_project(project_config);
        }

        // Add hosts from config
        for host_group_config in simulator.sim_config.clients.clone() {
            if host_group_config.trace.is_none() && host_group_config.cpu.is_none()
                || host_group_config.trace.is_some() && host_group_config.cpu.is_some()
            {
                panic!("Client group should be configered either with traces or manually");
            }
            let reliability_dist = SimulationDistribution::new(
                host_group_config
                    .reliability_distribution
                    .clone()
                    .unwrap_or(DistributionConfig::Uniform { min: 0.8, max: 1. }),
            );
            if host_group_config.cpu.is_some() {
                let count = host_group_config.count.clone().unwrap_or(1);
                for _ in 0..count {
                    simulator.add_host(
                        host_group_config.clone(),
                        simulator.ctx.sample_from_distribution(&reliability_dist),
                    );
                }
            } else {
                let trace_path = host_group_config.trace.clone().unwrap();
                let file = File::open(trace_path.clone()).unwrap();
                let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

                for result in reader.records() {
                    // cores, speed
                    let record = result.unwrap();

                    let resources = ClientCpuPower {
                        cores: record.get(0).unwrap().parse::<u32>().unwrap(),
                        speed: record.get(1).unwrap().parse::<f64>().unwrap(),
                    };

                    let mut host_config = host_group_config.clone();
                    host_config.cpu = Some(resources);

                    simulator.add_host(
                        host_config,
                        simulator.ctx.sample_from_distribution(&reliability_dist),
                    );
                }
            }
        }

        simulator
    }

    pub fn get_simulation(&self) -> Ref<Simulation> {
        self.simulation.borrow()
    }

    pub fn get_simulation_mut(&self) -> RefMut<Simulation> {
        self.simulation.borrow_mut()
    }

    pub fn run(self: Rc<Self>) {
        println!("Simulation started");

        self.get_simulation().spawn(self.clone().pre_run());

        let t = Instant::now();
        self.simulation.borrow_mut().step_until_no_events();
        let duration = t.elapsed().as_secs_f64();

        println!("Simulation finished");
        println!("");
        println!("******** Simulation Stats **********");
        println!("Time period simulated: {} h", self.sim_config.sim_duration);
        println!("Elapsed time: {:.2}s", duration);
        println!(
            "Simulation speedup: {:.2}",
            self.get_simulation().time() / duration
        );
        let event_count = self.get_simulation().event_count();
        println!(
            "Processed {} events in {:.2?}s ({:.0} events/s)",
            event_count,
            duration,
            event_count as f64 / duration
        );

        self.print_stats();
    }

    pub async fn pre_run(self: Rc<Self>) {
        for (_, project) in self.projects.iter() {
            self.ctx
                .emit_now(GenerateJobs { cnt: 300000 }, project.server.ctx.id());

            self.ctx.recv_event::<JobsGenerationCompleted>().await;

            log_info!(self.ctx, "Initial workunits added to {} server");

            self.ctx.emit_now(
                StartServer {
                    finish_time: self.ctx.time() + self.sim_config.sim_duration * 3600.,
                },
                project.server.ctx.id(),
            );
        }
        for client in &self.clients {
            self.ctx.emit_now(
                StartClient {
                    finish_time: self.ctx.time() + self.sim_config.sim_duration * 3600.,
                },
                client.ctx.id(),
            );
        }
    }

    pub fn add_project(&mut self, project_config: ProjectConfig) {
        let server_name = format!("{}::server", project_config.name);

        let stats = rc!(refcell!(ServerStats::default()));

        // Database
        let database = rc!(BoincDatabase::new());

        // Job generator
        let job_generator_name = &format!("{}::job_generator", server_name);
        let job_generator = JobGenerator::new(
            self.get_simulation_mut().create_context(job_generator_name),
            project_config.server.job_generator.clone(),
        );

        // Adding daemon components
        // Validator
        let validator_name = &format!("{}::validator", server_name);
        let validator = Validator::new(
            database.clone(),
            self.get_simulation_mut().create_context(validator_name),
            project_config.server.validator.clone(),
            stats.clone(),
        );

        // Transitioner
        let transitioner_name = &format!("{}::transitioner", server_name);
        let transitioner = Transitioner::new(
            database.clone(),
            self.get_simulation_mut().create_context(transitioner_name),
        );

        // Database purger
        let db_purger_name = &format!("{}::db_purger", server_name);
        let db_purger = DBPurger::new(
            database.clone(),
            self.get_simulation_mut().create_context(db_purger_name),
            stats.clone(),
        );

        // Feeder
        let shared_memory: Rc<RefCell<VecDeque<ResultId>>> = rc!(refcell!(VecDeque::new()));

        let feeder_name = &format!("{}::feeder", server_name);
        let feeder: Feeder = Feeder::new(
            shared_memory.clone(),
            database.clone(),
            self.get_simulation_mut().create_context(feeder_name),
            project_config.server.feeder.clone(),
        );

        // Scheduler
        let scheduler_name = &format!("{}::scheduler", server_name);
        let scheduler = rc!(refcell!(ServerScheduler::new(
            self.network.clone(),
            database.clone(),
            shared_memory.clone(),
            SimulationDistribution::new(
                project_config
                    .server
                    .scheduler
                    .clone()
                    .est_runtime_error_distribution
                    .unwrap_or(SCHEDULER_EST_RUNTIME_ERROR),
            ),
            self.get_simulation_mut().create_context(scheduler_name),
            stats.clone(),
        )));

        // Data server
        let data_server_name = &format!("{}::data_server", server_name);
        // file storage
        let disk_name = format!("{}::disk", data_server_name);
        let disk = rc!(refcell!(DiskBuilder::simple(
            project_config.server.data_server.disk_capacity * 1000,
            project_config.server.data_server.disk_read_bandwidth,
            project_config.server.data_server.disk_write_bandwidth
        )
        .build(self.get_simulation_mut().create_context(&disk_name))));
        self.get_simulation_mut()
            .add_handler(disk_name, disk.clone());

        let data_server = rc!(DataServer::new(
            self.network.clone(),
            disk,
            self.get_simulation_mut().create_context(data_server_name),
        ));
        let data_server_id = self
            .get_simulation_mut()
            .add_static_handler(data_server_name, data_server.clone());

        // Assimilator
        let assimilator_name = &format!("{}::assimilator", server_name);
        let assimilator = Assimilator::new(
            database.clone(),
            data_server.clone(),
            self.get_simulation_mut().create_context(assimilator_name),
        );

        // File deleter
        let file_deleter_name = &format!("{}::file_deleter", server_name);
        let file_deleter = FileDeleter::new(
            database.clone(),
            data_server.clone(),
            self.get_simulation_mut().create_context(file_deleter_name),
        );

        let server = rc!(ProjectServer::new(
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
            self.get_simulation_mut().create_context(&server_name),
            project_config.server.clone(),
            stats.clone()
        ));

        let server_id = self
            .get_simulation_mut()
            .add_static_handler(&server_name, server.clone());

        self.network.borrow_mut().add_node(
            &server_name,
            Box::new(SharedBandwidthNetworkModel::new(
                project_config.server.local_bandwidth,
                project_config.server.local_latency / 1000.,
            )),
        );
        self.network
            .borrow_mut()
            .set_location(data_server_id, &server_name);
        self.network
            .borrow_mut()
            .set_location(server_id, &server_name);

        self.projects.insert(
            project_config.name.clone(),
            BoincProject::new(project_config.name, server),
        );
    }

    pub fn add_host(&mut self, config: ClientGroupConfig, reliability: f64) {
        let n = self.clients.len();
        let node_name = &format!("client{}", n);

        //stats
        let stats = rc!(refcell!(ClientStats::new()));
        // compute
        let resources = config.cpu.clone().unwrap();
        let compute_name = format!("{}::compute", node_name);
        let compute = rc!(refcell!(Compute::new(
            resources.speed,
            resources.cores,
            config.memory * 1000,
            self.get_simulation_mut().create_context(&compute_name),
        )));
        self.get_simulation_mut()
            .add_handler(compute_name, compute.clone());
        // disk
        let disk_name = format!("{}::disk", node_name);
        let disk = rc!(refcell!(DiskBuilder::simple(
            config.disk_capacity * 1000,
            config.disk_read_bandwidth,
            config.disk_write_bandwidth
        )
        .build(self.get_simulation_mut().create_context(&disk_name))));
        self.get_simulation_mut()
            .add_handler(disk_name, disk.clone());

        let client_name = &format!("{}::client", node_name);

        // File Storage
        let file_storage: Rc<FileStorage> = rc!(FileStorage::new());

        let utilities = rc!(refcell!(Utilities::new(compute.clone(),)));

        // RR Simulator
        let rr_simulator_name = &format!("{}::rr_simulator", client_name);
        let rr_simulator = rc!(refcell!(RRSimulation::new(
            config.buffered_work_min,
            config.buffered_work_max,
            file_storage.clone(),
            compute.clone(),
            utilities.clone(),
            self.get_simulation_mut().create_context(rr_simulator_name),
        )));

        let all_projects = self.projects.borrow();
        let mut client_projects = HashMap::new();

        for supported_project in config.supported_projects.iter() {
            let project = all_projects.get(&supported_project.name).unwrap();
            let server_id = project.server.ctx.id();
            let data_server_id = project.server.data_server.ctx.id();
            client_projects.insert(
                server_id,
                ProjectInfo {
                    server_id,
                    data_server_id,
                    resource_share: supported_project.resource_share,
                },
            );
        }

        let client = rc!(Client::new(
            compute,
            disk,
            self.network.clone(),
            client_projects,
            utilities.clone(),
            rr_simulator.clone(),
            file_storage.clone(),
            self.get_simulation_mut().create_context(client_name),
            config.clone(),
            reliability,
            SimulationDistribution::new(
                config
                    .availability_distribution
                    .unwrap_or(CLIENT_AVAILABILITY_ALL_RANDOM_HOSTS.availability),
            ),
            SimulationDistribution::new(
                config
                    .unavailability_distribution
                    .unwrap_or(CLIENT_AVAILABILITY_ALL_RANDOM_HOSTS.unavailability),
            ),
            stats.clone(),
        ));

        let client_id = self
            .get_simulation_mut()
            .add_static_handler(client_name, client.clone());

        self.network.borrow_mut().add_node(
            node_name,
            Box::new(SharedBandwidthNetworkModel::new(
                config.local_bandwidth,
                config.local_latency / 1000.,
            )),
        );
        self.network
            .borrow_mut()
            .set_location(client_id, &node_name);

        self.clients.push(client.clone());
    }

    pub fn print_stats(&self) {
        for (_, project) in self.projects.iter() {
            print_project_stats(project, self.sim_config.sim_duration);
        }
        print_clients_stats(self.clients.clone(), self.sim_config.sim_duration);
    }
}

struct EmptyEventHandler {}

impl EventHandler for EmptyEventHandler {
    fn on(&mut self, _event: Event) {}
}
