use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::Simulation;
use dslab_network::{
    models::{ConstantBandwidthNetworkModel, SharedBandwidthNetworkModel},
    Network, NetworkModel,
};
use rand_pcg::Pcg64;
use std::cell::RefCell;
use std::rc::Rc;
use sugars::{boxed, rc, refcell};

pub struct Simulator {
    id: Id,
    sim: Rc<RefCell<Simulation>>,
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl Simulator {
    // pub fn new(
    //     seed: u64,
    //     use_shared_network: bool,
    //     network_bandwidth: f64,
    //     network_latency: f64,
    //     ctx: SimulationContext,
    // ) -> Self {
    //     let mut sim = Simulation::new(seed);
    //     let mut rand = Pcg64::seed_from_u64(seed);

    //     // admin context for starting job generator, server and clients
    //     let admin = sim.create_context("admin");

    //     let network_model: Box<dyn NetworkModel> = if use_shared_network {
    //         boxed!(SharedBandwidthNetworkModel::new(
    //             network_bandwidth as f64,
    //             network_latency
    //         ))
    //     } else {
    //         boxed!(ConstantBandwidthNetworkModel::new(
    //             network_bandwidth as f64,
    //             network_latency
    //         ))
    //     };
    //     let network = rc!(refcell!(Network::new(
    //         network_model,
    //         sim.create_context("net")
    //     )));
    //     sim.add_handler("net", network.clone());
    //     return Self {
    //         id: ctx.id(),
    //         rc!(refcell!(sim)),
    //         rc!(refcell!(net)),
    //         ctx,
    //     };
    // }
}
