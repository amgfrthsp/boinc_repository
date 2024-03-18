use dslab_compute::multicore::Compute;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::Simulation;
use dslab_network::{
    models::{ConstantBandwidthNetworkModel, SharedBandwidthNetworkModel},
    Network, NetworkModel,
};
use dslab_storage::disk::DiskBuilder;
use rand::prelude::*;
use rand_pcg::{Lcg128Xsl64, Pcg64};
use std::rc::Rc;
use std::{cell::RefCell, time::Instant};
use sugars::{boxed, rc, refcell};

use crate::{
    client::client::Client,
    common::Start,
    server::{
        assimilator::Assimilator, database::BoincDatabase, job_generator::JobGenerator,
        scheduler::Scheduler, server::Server, transitioner::Transitioner, validator::Validator,
    },
};

pub struct Simulator {
    id: Id,
    sim: Rc<RefCell<Simulation>>,
    net: Rc<RefCell<Network>>,
    rand: Lcg128Xsl64,
    hosts: Vec<String>,
    job_generator_id: Option<u32>,
    server_id: Option<u32>,
    client_ids: Vec<u32>,
    ctx: SimulationContext,
}

impl Simulator {
    pub fn new(
        seed: u64,
        use_shared_network: bool,
        network_bandwidth: f64,
        network_latency: f64,
    ) -> Self {
        let mut sim = Simulation::new(seed);
        let rand = Pcg64::seed_from_u64(seed);

        let network_model: Box<dyn NetworkModel> = if use_shared_network {
            boxed!(SharedBandwidthNetworkModel::new(
                network_bandwidth as f64,
                network_latency
            ))
        } else {
            boxed!(ConstantBandwidthNetworkModel::new(
                network_bandwidth as f64,
                network_latency
            ))
        };
        let network = rc!(refcell!(Network::new(
            network_model,
            sim.create_context("net")
        )));
        sim.add_handler("net", network.clone());

        // context for starting job generator, server and clients
        let ctx = sim.create_context("ctx");

        return Self {
            id: ctx.id(),
            sim: rc!(refcell!(sim)),
            net: network,
            rand,
            hosts: Vec::new(),
            job_generator_id: None,
            server_id: None,
            client_ids: Vec::new(),
            ctx,
        };
    }

    pub fn run(&mut self) {
        if self.job_generator_id.is_none() {
            println!("Job Generator is not added");
            return;
        }
        if self.server_id.is_none() {
            println!("Server is not added");
            return;
        }
        self.ctx.emit_now(Start {}, self.job_generator_id.unwrap());
        self.ctx.emit_now(Start {}, self.server_id.unwrap());
        for client_id in &self.client_ids {
            self.ctx.emit_now(Start {}, *client_id);
        }

        let t = Instant::now();
        self.sim.borrow_mut().step_until_no_events();
        let duration = t.elapsed().as_secs_f64();
        println!("Elapsed time: {:.2}s", duration);
        //println!("Scheduling time: {:.2}s", server.borrow().scheduling_time);
        println!(
            "Simulation speedup: {:.2}",
            self.sim.borrow().time() / duration
        );
        println!(
            "Processed {} events in {:.2?}s ({:.0} events/s)",
            self.sim.borrow().event_count(),
            duration,
            self.sim.borrow().event_count() as f64 / duration
        );
    }

    pub fn add_job_generator(&mut self, local_bandwidth: f64, local_latency: f64) {
        let n = self.hosts.len();
        let node_name = &format!("host{}", n);
        self.net.borrow_mut().add_node(
            node_name,
            Box::new(SharedBandwidthNetworkModel::new(
                local_bandwidth,
                local_latency,
            )),
        );
        self.hosts.push(node_name.to_string());
        let job_generator_name = &format!("{}::job_generator", node_name);
        let job_generator = rc!(refcell!(JobGenerator::new(
            self.net.clone(),
            self.sim.borrow_mut().create_context(job_generator_name)
        )));
        let job_generator_id = self
            .sim
            .borrow_mut()
            .add_handler(job_generator_name, job_generator.clone());
        self.net
            .borrow_mut()
            .set_location(job_generator_id, node_name);
        self.job_generator_id = Some(job_generator_id);
    }

    pub fn add_server(&mut self, local_bandwidth: f64, local_latency: f64) {
        let n = self.hosts.len();
        let node_name = &format!("host{}", n);
        self.net.borrow_mut().add_node(
            node_name,
            Box::new(SharedBandwidthNetworkModel::new(
                local_bandwidth as f64,
                local_latency,
            )),
        );
        self.hosts.push(node_name.to_string());
        let server_name = &format!("{}::server", node_name);

        // Creating database
        let database = rc!(BoincDatabase::new());

        // Adding daemon components
        // Validator
        let validator_name = &format!("{}::validator", server_name);
        let validator = rc!(refcell!(Validator::new(
            database.clone(),
            self.sim.borrow_mut().create_context(validator_name)
        )));
        // Assimilator
        let assimilator_name = &format!("{}::assimilator", server_name);
        let assimilator = rc!(refcell!(Assimilator::new(
            database.clone(),
            self.sim.borrow_mut().create_context(assimilator_name)
        )));
        // Transitioner
        let transitioner_name = &format!("{}::transitioner", server_name);
        let transitioner = rc!(refcell!(Transitioner::new(
            database.clone(),
            self.sim.borrow_mut().create_context(transitioner_name)
        )));
        // Scheduler
        let scheduler_name = &format!("{}::scheduler", server_name);
        let scheduler = rc!(refcell!(Scheduler::new(
            self.net.clone(),
            database.clone(),
            self.sim.borrow_mut().create_context(scheduler_name)
        )));
        let scheduler_id = self
            .sim
            .borrow_mut()
            .add_handler(scheduler_name, scheduler.clone());
        self.net.borrow_mut().set_location(scheduler_id, node_name);
        let server = rc!(refcell!(Server::new(
            self.net.clone(),
            database.clone(),
            validator,
            assimilator,
            transitioner,
            scheduler,
            self.job_generator_id.unwrap(),
            self.sim.borrow_mut().create_context(server_name)
        )));
        let server_id = self
            .sim
            .borrow_mut()
            .add_handler(server_name, server.clone());
        self.net.borrow_mut().set_location(server_id, node_name);
        self.server_id = Some(server_id);
    }

    pub fn add_host(
        &mut self,
        local_bandwidth: f64,
        local_latency: f64,
        disk_capacity: u64,
        disk_read_bandwidth: f64,
        disk_write_bandwidth: f64,
    ) {
        let n = self.hosts.len();
        let node_name = &format!("host{}", n);
        self.net.borrow_mut().add_node(
            node_name,
            Box::new(SharedBandwidthNetworkModel::new(
                local_bandwidth as f64,
                local_latency,
            )),
        );
        self.hosts.push(node_name.to_string());
        // compute
        let compute_name = format!("{}::compute", node_name);
        let compute = rc!(refcell!(Compute::new(
            self.rand.gen_range(1..=10) as f64,
            self.rand.gen_range(1..=8),
            self.rand.gen_range(1..=4) * 1024,
            self.sim.borrow_mut().create_context(&compute_name),
        )));
        self.sim
            .borrow_mut()
            .add_handler(compute_name, compute.clone());
        // disk
        let disk_name = format!("{}::disk", node_name);
        let disk = rc!(refcell!(DiskBuilder::simple(
            disk_capacity,
            disk_read_bandwidth,
            disk_write_bandwidth
        )
        .build(self.sim.borrow_mut().create_context(&disk_name))));
        self.sim.borrow_mut().add_handler(disk_name, disk.clone());

        let client_name = &format!("{}::client", node_name);
        let client = Client::new(
            compute,
            disk,
            self.net.clone(),
            self.server_id.unwrap(),
            self.sim.borrow_mut().create_context(client_name),
        );
        let client_id = self
            .sim
            .borrow_mut()
            .add_handler(client_name, rc!(refcell!(client)));
        self.net.borrow_mut().set_location(client_id, node_name);
        self.client_ids.push(client_id);
    }
}
