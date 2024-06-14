use std::rc::Rc;

use dslab_storage::storage::Storage;

use crate::{
    client::{client::Client, stats::ClientStats},
    common::HOUR,
    project::{
        job::{AssimilateState, ResultOutcome, ResultState, ValidateState},
        project::BoincProject,
    },
};

pub fn print_project_stats(project: &BoincProject, sim_duration: f64) {
    let stats = project.server.stats.borrow();
    let workunits = project.server.db.workunit.borrow();
    let results = project.server.db.result.borrow();

    println!("******** {} Server Stats **********", project.name);
    println!(
        "Average Speed: {:.2} GFLOPS",
        stats.gflops_total / (sim_duration * 3600.)
    );
    println!("Total credit granted: {:.3}", stats.total_credit_granted);
    println!(
        "Average credit/day: {:.3}",
        stats.total_credit_granted / (sim_duration / 24.)
    );
    // println!("Assimilator sum dur: {:.2} s", stats.assimilator_sum_dur);
    // println!("Validator sum dur: {:.2} s", stats.validator_sum_dur);
    // println!("Transitioner sum dur: {:.2} s", stats.transitioner_sum_dur);
    // println!("Feeder sum dur: {:.2} s", stats.feeder_sum_dur);
    // println!("Scheduler sum dur: {:.2} s", stats.scheduler_sum_dur);
    // println!("File deleter sum dur: {:.2} s", stats.file_deleter_sum_dur);
    // println!("DB purger sum dur: {:.2} s", stats.db_purger_sum_dur);
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

pub fn print_clients_stats(clients: Vec<Rc<Client>>, sim_duration: f64) {
    let mut total_stats = ClientStats::new();

    let mut cores_sum = 0;
    let mut speed_sum = 0.;
    let mut memory_sum = 0;
    let mut disk_sum = 0;

    let n_clients = clients.len();

    for client in clients {
        total_stats += client.stats.borrow().clone();

        cores_sum += client.compute.borrow().cores_total();
        memory_sum += client.compute.borrow().memory_total();
        speed_sum += client.compute.borrow().speed();
        disk_sum += client.disk.borrow().capacity();
    }

    println!("******** Clients Stats **********");
    println!("Total number of clients: {}", n_clients);
    println!("RR sim sum dur: {:.2} s", total_stats.rrsim_sum_dur);
    println!(
        "RR sim average dur: {:.2} s",
        total_stats.rrsim_sum_dur / n_clients as f64
    );
    println!("Sched sum dur: {:.2} s", total_stats.scheduler_sum_dur);
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
        (total_stats.time_available as f64 / (sim_duration * 3600.)) / n_clients as f64 * 100.
    );
    println!(
        "- Average host unavailability: {:.2}%",
        (total_stats.time_unavailable as f64 / (sim_duration * 3600.)) / n_clients as f64 * 100.
    );
    println!(
        "- Average results processed by one client: {:.2}",
        total_stats.n_results_processed as f64 / n_clients as f64
    );
    println!("");
}
