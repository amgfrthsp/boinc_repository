# Boinc System Simulator
## Manual 
1. Set up config file. Example can be found in [config.yaml](examples/simple/config.yaml). Brief description of each parameter can be found in [sim_config.rs](simulator/src/config/sim_config.rs).
2. Run with a command `RUST_LOG=<LOGGING_MODE> cargo run --release --  2> output.txt`, where LOGGING_MODE configures what simulation logs are printed in your output file. It can have the following values: 
    1. `ERROR` (default) -- prints only erroneous events
    2. `INFO` -- prints only `log_info` logs: logs which indicate main simulation events, including erroneous events
    3. `DEBUG` --  prints only `log_debug` logs: extended simulation pipeline events, including info-logs 
    4. `TRACE`  -- prints all simulation logs and dslab events 
