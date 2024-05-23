# Boinc System Simulator
## Manual 
1. Pull DSLab repository at the same folder as the simulator
2. Checkout to DSLab `compute-preemption` branch
3. Run with a command `RUST_LOG=<LOGGING_MODE> cargo run --release --  2> output.txt`, where LOGGING_MODE configures what simulation logs are printed in your output file. It can have the following values: 
    1. `ERROR` (default) -- prints only erroneous events
    2. `INFO` -- prints only `log_info` logs: logs which indicate main simulation events, including erroneous events
    3. `DEBUG` --  prints only `log_debug` logs: extended simulation pipeline events, including info-logs 
    4. `TRACE`  -- prints all simulation logs and dslab events 
