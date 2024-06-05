use csv::ReaderBuilder;
use dslab_compute::multicore::Compute;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::{log_info, Event, EventHandler, Simulation};
use dslab_network::{models::SharedBandwidthNetworkModel, Network};
use dslab_storage::disk::DiskBuilder;
use dslab_storage::storage::Storage;
use serde::Serialize;
use std::collections::VecDeque;
use std::fs::File;
use std::rc::Rc;
use std::{cell::RefCell, time::Instant};
use sugars::{rc, refcell};

use crate::client::rr_simulation::RRSimulation;
use crate::client::stats::ClientStats;
use crate::client::storage::FileStorage;
use crate::client::utils::Utilities;
use crate::common::HOUR;
use crate::config::sim_config::{
    ClientCpuPower, ClientGroupConfig, ServerConfig, SimulationConfig,
};
use crate::server::db_purger::DBPurger;
use crate::server::feeder::Feeder;
use crate::server::file_deleter::FileDeleter;
use crate::server::job::{AssimilateState, ResultId, ResultOutcome, ResultState, ValidateState};
use crate::server::server::{GenerateJobs, JobsGenerationCompleted};
use crate::server::stats::ServerStats;
use crate::{
    client::client::Client,
    server::{
        assimilator::Assimilator, data_server::DataServer, database::BoincDatabase,
        job_generator::JobGenerator, scheduler::Scheduler as ServerScheduler, server::Server,
        transitioner::Transitioner, validator::Validator,
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
    pub server_id: Id,
    pub data_server_id: Id,
    pub finish_time: f64,
}

pub struct Simulator {
    simulation: Simulation,
    server: Option<Rc<RefCell<Server>>>,
    clients: Vec<Rc<RefCell<Client>>>,
    ctx: SimulationContext,
    sim_config: SimulationConfig,
}

impl Simulator {
    pub fn new(sim_config: SimulationConfig) -> Self {
        let mut simulation = Simulation::new(sim_config.seed);

        // context for starting server and clients
        let ctx = simulation.create_context("ctx");
        simulation.add_handler("ctx", rc!(refcell!(EmptyEventHandler {})));

        let mut simulator = Self {
            simulation,
            server: None,
            clients: Vec::new(),
            ctx,
            sim_config,
        };

        simulator.add_server(simulator.sim_config.server.clone());
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

        let server = simulator.server.clone().unwrap();
        let server_node_name = "server";

        for client_ref in &simulator.clients {
            let client = client_ref.borrow_mut();
            let client_network = client.get_network();

            client_network.borrow_mut().add_node(
                server_node_name,
                Box::new(SharedBandwidthNetworkModel::new(
                    simulator.sim_config.server.local_bandwidth,
                    simulator.sim_config.server.local_latency / 1000.,
                )),
            );

            server.borrow().add_client_network(
                client.ctx.id(),
                client_network.clone(),
                server_node_name,
            );
        }

        simulator
    }

    pub fn run(&mut self) {
        if self.server.is_none() {
            println!("Server is not added");
            return;
        }
        println!("Simulation started");

        let server = self.server.clone().unwrap();

        self.ctx.spawn(async {
            self.ctx
                .emit_now(GenerateJobs { cnt: 300000 }, server.borrow().ctx.id());

            self.ctx.recv_event::<JobsGenerationCompleted>().await;

            log_info!(self.ctx, "Initial workunits added to server");

            self.ctx.emit_now(
                StartServer {
                    finish_time: self.ctx.time() + self.sim_config.sim_duration * 3600.,
                },
                server.borrow().ctx.id(),
            );

            for client in &self.clients {
                self.ctx.emit_now(
                    StartClient {
                        server_id: server.borrow().ctx.id(),
                        data_server_id: server.borrow().data_server.borrow().ctx.id(),
                        finish_time: self.ctx.time() + self.sim_config.sim_duration * 3600.,
                    },
                    client.borrow().ctx.id(),
                );
            }
        });

        let server = self.server.clone().unwrap();

        let t = Instant::now();
        self.simulation.step_until_no_events();
        let duration = t.elapsed().as_secs_f64();

        println!("Simulation finished");
        println!("");
        println!("Elapsed time: {:.2}s", duration);
        println!(
            "Memory usage: {:.2} GB",
            server.borrow().memory / 1_000_000_000.
        );
        println!("Total number of clients: {}", self.clients.len());
        println!(
            "Simulation speedup: {:.2}",
            self.simulation.time() / duration
        );
        let event_count = self.simulation.event_count();
        println!(
            "Processed {} events in {:.2?}s ({:.0} events/s)",
            event_count,
            duration,
            event_count as f64 / duration
        );

        self.print_stats();
    }

    pub fn add_server(&mut self, config: ServerConfig) {
        let server_name = "server";

        let stats = rc!(refcell!(ServerStats::default()));

        // Database
        let database = rc!(BoincDatabase::new());

        // Job generator
        let job_generator_name = &format!("{}::job_generator", server_name);
        let job_generator = JobGenerator::new(
            self.simulation.create_context(job_generator_name),
            config.job_generator.clone(),
        );

        // Adding daemon components
        // Validator
        let validator_name = &format!("{}::validator", server_name);
        let validator = Validator::new(
            database.clone(),
            self.simulation.create_context(validator_name),
            config.validator.clone(),
            stats.clone(),
        );

        // Transitioner
        let transitioner_name = &format!("{}::transitioner", server_name);
        let transitioner = Transitioner::new(
            database.clone(),
            self.simulation.create_context(transitioner_name),
        );

        // Database purger
        let db_purger_name = &format!("{}::db_purger", server_name);
        let db_purger = DBPurger::new(
            database.clone(),
            self.simulation.create_context(db_purger_name),
            stats.clone(),
        );

        // Feeder
        let shared_memory: Rc<RefCell<VecDeque<ResultId>>> = rc!(refcell!(VecDeque::new()));

        let feeder_name = &format!("{}::feeder", server_name);
        let feeder: Feeder = Feeder::new(
            shared_memory.clone(),
            database.clone(),
            self.simulation.create_context(feeder_name),
            config.feeder.clone(),
        );

        // Scheduler
        let scheduler_name = &format!("{}::scheduler", server_name);
        let scheduler = rc!(refcell!(ServerScheduler::new(
            database.clone(),
            shared_memory.clone(),
            SimulationDistribution::new(
                config
                    .scheduler
                    .clone()
                    .est_runtime_error_distribution
                    .unwrap_or(SCHEDULER_EST_RUNTIME_ERROR),
            ),
            self.simulation.create_context(scheduler_name),
            stats.clone(),
        )));

        // Data server
        let data_server_name = &format!("{}::data_server", server_name);
        // file storage
        let disk_name = format!("{}::disk", data_server_name);
        let disk = rc!(refcell!(DiskBuilder::simple(
            config.data_server.disk_capacity * 1000,
            config.data_server.disk_read_bandwidth,
            config.data_server.disk_write_bandwidth
        )
        .build(self.simulation.create_context(&disk_name))));
        self.simulation.add_handler(disk_name, disk.clone());

        let data_server: Rc<RefCell<DataServer>> = rc!(refcell!(DataServer::new(
            disk,
            self.simulation.create_context(data_server_name),
        )));
        self.simulation
            .add_handler(data_server_name, data_server.clone());

        // Assimilator
        let assimilator_name = &format!("{}::assimilator", server_name);
        let assimilator = Assimilator::new(
            database.clone(),
            data_server.clone(),
            self.simulation.create_context(assimilator_name),
        );

        // File deleter
        let file_deleter_name = &format!("{}::file_deleter", server_name);
        let file_deleter = FileDeleter::new(
            database.clone(),
            data_server.clone(),
            self.simulation.create_context(file_deleter_name),
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
            self.simulation.create_context(server_name),
            config,
            stats.clone()
        )));

        self.simulation.add_handler(server_name, server.clone());
        self.server = Some(server.clone());
    }

    pub fn add_host(&mut self, config: ClientGroupConfig, reliability: f64) {
        let n = self.clients.len();
        let node_name = &format!("client{}", n);

        let network_name = &format!("{}::network", node_name);
        let network = rc!(refcell!(Network::new(
            Box::new(SharedBandwidthNetworkModel::new(
                config.network_bandwidth,
                config.network_latency / 1000.
            )),
            self.simulation.create_context(network_name),
        )));

        self.simulation.add_handler(network_name, network.clone());

        network.borrow_mut().add_node(
            node_name,
            Box::new(SharedBandwidthNetworkModel::new(
                config.local_bandwidth,
                config.local_latency / 1000.,
            )),
        );

        //stats
        let stats = rc!(refcell!(ClientStats::new()));
        // compute
        let resources = config.cpu.clone().unwrap();
        let compute_name = format!("{}::compute", node_name);
        let compute = rc!(refcell!(Compute::new(
            resources.speed,
            resources.cores,
            config.memory * 1000,
            self.simulation.create_context(&compute_name),
        )));
        self.simulation.add_handler(compute_name, compute.clone());
        // disk
        let disk_name = format!("{}::disk", node_name);
        let disk = rc!(refcell!(DiskBuilder::simple(
            config.disk_capacity * 1000,
            config.disk_read_bandwidth,
            config.disk_write_bandwidth
        )
        .build(self.simulation.create_context(&disk_name))));
        self.simulation.add_handler(disk_name, disk.clone());

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
            self.simulation.create_context(rr_simulator_name),
        )));

        let client = rc!(refcell!(Client::new(
            compute,
            disk,
            network.clone(),
            node_name.to_string(),
            utilities.clone(),
            rr_simulator.clone(),
            file_storage.clone(),
            self.simulation.create_context(client_name),
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
        )));

        let client_id = self.simulation.add_handler(client_name, client.clone());
        network.borrow_mut().set_location(client_id, node_name);

        self.clients.push(client.clone());
    }

    pub fn print_stats(&self) {
        let server_clone = self.server.clone().unwrap();
        let server = server_clone.borrow();
        let stats = server.stats.borrow();

        println!("");
        println!("******** Simulation Stats **********");
        println!("Time period simulated: {} h", self.sim_config.sim_duration);
        println!("Calculated {:.3} PFLOPS", stats.gflops_total / 1_000_000.);
        println!("Total credit granted: {:.3}", stats.total_credit_granted);
        println!("");

        self.print_server_stats();
        self.print_client_stats();
    }

    pub fn print_server_stats(&self) {
        let server_clone = self.server.clone().unwrap();
        let server = server_clone.borrow();
        let stats = server.stats.borrow();
        let workunits = server.db.workunit.borrow();
        let results = server.db.result.borrow();

        println!("******** Server Stats **********");
        println!("Assimilator sum dur: {:.2} s", stats.assimilator_sum_dur);
        println!("Validator sum dur: {:.2} s", stats.validator_sum_dur);
        println!("Transitioner sum dur: {:.2} s", stats.transitioner_sum_dur);
        println!("Feeder sum dur: {:.2} s", stats.feeder_sum_dur);
        println!("Scheduler sum dur: {:.2} s", stats.scheduler_sum_dur);
        println!("File deleter sum dur: {:.2} s", stats.file_deleter_sum_dur);
        println!("DB purger sum dur: {:.2} s", stats.db_purger_sum_dur);
        // println!(
        //     "Empty buffer: {}. shmem size {} lower bound {}",
        //     server.scheduler.borrow().dur_samples,
        //     self.sim_config.server.feeder.shared_memory_size,
        //     UNSENT_RESULT_BUFFER_LOWER_BOUND
        // );
        println!("");

        let mut n_wus_inprogress = 0;
        let mut n_wus_stage_canonical = 0;
        let mut n_wus_stage_assimilation = 0;
        let mut n_wus_stage_deletion = 0;

        for wu_item in workunits.iter() {
            let wu = wu_item.1;
            let mut at_least_one_result_sent = false;
            for result_id in &wu.result_ids {
                let res_opt = results.get(result_id);
                if res_opt.is_none() {
                    at_least_one_result_sent = true;
                } else {
                    let res = res_opt.unwrap();
                    if res.server_state != ResultState::Unsent {
                        at_least_one_result_sent = true;
                    }
                }
            }
            if !at_least_one_result_sent {
                continue;
            }
            n_wus_inprogress += 1;

            if wu.canonical_resultid.is_none() {
                n_wus_stage_canonical += 1;
                continue;
            }
            if wu.assimilate_state != AssimilateState::Done {
                n_wus_stage_assimilation += 1;
                continue;
            }
            n_wus_stage_deletion += 1;
        }
        let n_wus_total = n_wus_inprogress + stats.n_workunits_fully_processed;

        println!("******** Workunit Stats **********");
        println!("Workunits in db: {}", workunits.len());
        println!("Workunits total: {}", n_wus_total);
        println!(
            "- Workunits waiting for canonical result: {:.2}%",
            n_wus_stage_canonical as f64 / n_wus_total as f64 * 100.
        );
        println!(
            "- Workunits waiting for assimilation: {:.2}%",
            n_wus_stage_assimilation as f64 / n_wus_total as f64 * 100.
        );
        println!(
            "- Workunits waiting for deletion: {} = {:.2}%",
            n_wus_stage_deletion,
            n_wus_stage_deletion as f64 / n_wus_total as f64 * 100.
        );
        println!(
            "- Workunits fully processed: {} = {:.2}%",
            stats.n_workunits_fully_processed,
            stats.n_workunits_fully_processed as f64 / n_wus_total as f64 * 100.
        );
        println!("");

        println!("******** Results Stats **********");

        let mut n_res_in_progress = 0;
        let mut n_res_over = stats.n_res_deleted;
        let mut n_res_success = stats.n_res_success;
        let mut n_res_init = stats.n_res_init;
        let mut n_res_valid = stats.n_res_valid;
        let mut n_res_invalid = stats.n_res_invalid;
        let mut n_res_noreply = stats.n_res_noreply;
        let mut n_res_didntneed = stats.n_res_didntneed;
        let mut n_res_validateerror = stats.n_res_validateerror;

        for item in results.iter() {
            let result = item.1;
            if result.server_state == ResultState::InProgress {
                n_res_in_progress += 1;
                continue;
            }
            if result.server_state == ResultState::Over {
                match result.outcome {
                    ResultOutcome::Undefined => {
                        continue;
                    }
                    ResultOutcome::Success => {
                        n_res_success += 1;
                        match result.validate_state {
                            ValidateState::Valid => {
                                n_res_valid += 1;
                            }
                            ValidateState::Invalid => {
                                n_res_invalid += 1;
                            }
                            ValidateState::Init => {
                                n_res_init += 1;
                            }
                        }
                    }
                    ResultOutcome::NoReply => {
                        n_res_noreply += 1;
                    }
                    ResultOutcome::DidntNeed => {
                        n_res_didntneed += 1;
                    }
                    ResultOutcome::ValidateError => {
                        n_res_validateerror += 1;
                    }
                }
                n_res_over += 1;
            }
        }
        println!("- Results in progress: {}", n_res_in_progress);
        println!("- Results over: {}", n_res_over);
        println!(
            "-- Success: {:.2}%",
            n_res_success as f64 / n_res_over as f64 * 100.
        );
        println!(
            "--- Validate State = Init: {:.2}%",
            n_res_init as f64 / n_res_success as f64 * 100.
        );
        println!(
            "--- Validate State = Valid: {:.2}%, {:.2}%",
            n_res_valid as f64 / n_res_success as f64 * 100.,
            n_res_valid as f64 / (n_res_valid + n_res_invalid) as f64 * 100.
        );
        println!(
            "--- Validate State = Invalid: {:.2}%, {:.2}%",
            n_res_invalid as f64 / n_res_success as f64 * 100.,
            n_res_invalid as f64 / (n_res_valid + n_res_invalid) as f64 * 100.
        );
        println!(
            "-- Missed Deadline: {:.2}%",
            n_res_noreply as f64 / n_res_over as f64 * 100.
        );
        println!(
            "-- Sent to client, but server didn't need it: {:.2}%",
            n_res_didntneed as f64 / n_res_over as f64 * 100.
        );
        println!(
            "-- Validate Error: {:.2}%",
            n_res_validateerror as f64 / n_res_over as f64 * 100.
        );
        println!(
            "Average result processing time: {:.2} h",
            stats.results_processing_time / stats.n_results_completed as f64 / HOUR
        );
        println!(
            "Min result processing time: {:.2} h",
            stats.min_result_processing_time / HOUR
        );
        println!(
            "Max result processing time: {:.2} h",
            stats.max_result_processing_time / HOUR
        );
        println!("");
    }

    pub fn print_client_stats(&self) {
        let mut total_stats = ClientStats::new();

        let mut cores_sum = 0;
        let mut speed_sum = 0.;
        let mut memory_sum = 0;
        let mut disk_sum = 0;

        let mut rr_sim_dur = 0.;
        let mut sched_dur = 0.;

        for client_ref in &self.clients {
            let client = client_ref.borrow();
            total_stats += client.stats.borrow().clone();

            cores_sum += client.compute.borrow().cores_total();
            memory_sum += client.compute.borrow().memory_total();
            speed_sum += client.compute.borrow().speed();
            disk_sum += client.disk.borrow().capacity();

            rr_sim_dur += client.stats.borrow().rrsim_sum_sur;
            sched_dur += client.stats.borrow().scheduler_sum_sur;
        }

        let n_clients = self.clients.len();

        println!("******** Clients Stats **********");
        println!("Total number of clients: {}", self.clients.len());
        println!("RR sim sum dur: {:.2} s", rr_sim_dur);
        println!(
            "RR sim average dur: {:.2} s",
            rr_sim_dur / self.clients.len() as f64
        );
        println!("Sched sum dur: {:.2} s", sched_dur);
        println!(
            "- Average cores: {:.2}",
            cores_sum as f64 / n_clients as f64
        );
        println!(
            "- Average core speed: {:.2} GFLOPS/core",
            speed_sum as f64 / n_clients as f64
        );
        println!(
            "- Average memory: {:.2} GB",
            memory_sum as f64 / n_clients as f64 / 1000.
        );
        println!(
            "- Average disk capacity: {:.2} GB",
            disk_sum as f64 / n_clients as f64 / 1000.
        );
        println!(
            "- Average host availability: {:.2}%",
            (total_stats.time_available as f64 / (self.sim_config.sim_duration * 3600.))
                / n_clients as f64
                * 100.
        );
        println!(
            "- Average host unavailability: {:.2}%",
            (total_stats.time_unavailable as f64 / (self.sim_config.sim_duration * 3600.))
                / n_clients as f64
                * 100.
        );
        println!(
            "- Average GFLOPs processed: {:.2}",
            (total_stats.gflops_processed as f64 / n_clients as f64)
        );
        println!(
            "- Average results processed by one client: {:.2}",
            total_stats.n_results_processed as f64 / n_clients as f64
        );
        println!("");
    }
}

struct EmptyEventHandler {}

impl EventHandler for EmptyEventHandler {
    fn on(&mut self, _event: Event) {}
}
