# Workload gen cli

## Building

<https://www.rust-lang.org/tools/install>

```bash
cargo build --release
./target/release/workload-gen-cli
```

## Usage

```bash
./workload-gen-cli schema > workload_schema.json

./workload-gen-cli generate -w workload_spec.json
# or
./workload-gen-cli generate -w workload_spec.json -o workload_outputs/
# or 
./workload-gen-cli generate -w workload_specs/ -o workload_outputs/
```

```bash
Usage: workload-gen-cli <COMMAND>

Commands:
  generate  Generate workload(s) from a file or folder of workload specifications
  schema    Prints the json schmea for IDE integration
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
  
  
Usage: workload-gen-cli generate [OPTIONS] --workload <WORKLOAD_PATH>

Options:
  -w, --workload <WORKLOAD_PATH>  File or folder of workload spec files
  -o, --output <OUTPUT>           Output folder for workloads
  -h, --help                      Print help

```

## TODO

- [ ] point queries based on existing or non existing keys. generate in batches
- [ ] different distributions

- speedups
    - [ ] only sort / have data structures when they are necessary
    - [ ] only store keys + write to buffered writer immediately
    - [ ] only sort keys on rqs when there is an rq
    - [ ] use indexes instead of pointers
    - [ ] when comparing non-zero values, use multiple threads
    - [ ] io_uring

- [ ] run a check of the workload spec before generating to check for errors like more deletes than valid keys or having
  non-empty pqs without any inserts

- [ ] warnings about keyspace and how picking a small space could lead to lots of failed generation of empty point queries

- [ ] create some sort of workload planner (similar to a query planner) that chooses the correct data structure to use based on the combinations of operations
  - e.g. for empty point queries: deletes ? hash_set : bloom_filter. To check inclusion

### Extra Data structures

At a minimum, we need a `Vec<Option<Box[u8]>>` holding valid keys.

| Interleaving        | Inserts                                                                                                                                                                                                                                                                                                                             | Updates | Deletes                                             | Point Queries | Range Queries | Empty Point Queries |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|-----------------------------------------------------|---------------|---------------|---------------------|
| Inserts             | Append to valid keys                                                                                                                                                                                                                                                                                                                |         |                                                     |               |               |                     |
| Updates             | Append to valid keys and generate a random index to pick the update key based on a distribution.                                                                                                                                                                                                                                    |         |                                                     |               |               |                     |
| Deletes             | Append to valid keys. Delete by swapping an element to `None` in the `Vec`                                                                                                                                                                                                                                                          |         |                                                     |               |               |                     |
| Point Queries       | Append to valid keys. Pick a random index in valid keys.                                                                                                                                                                                                                                                                            |         |                                                     |               |               |                     |
| Range Queries       | Either store keys in a BTreeSet or sort valid keys before each range query. Pick an index between 0 and the max beginning of the valid range.                                                                                                                                                                                       |         | When sorting the valid keys, also filter out `None` |               |               |                     |
| Empty Point Queries | Generate the inserts, and then generate n empty queries. Find out how many invalid empty point queries, generate that many. Loop until all valid empty queries, shuffle the operations. Alternatively, instead of keeping all the operations in memory, keep offsets in the file of where you need to write and fill them in later. |         |                                                     |               |               |                     |

## Profiling

```bash
cargo flamegraph --unit-test workload_gen -- workload_1m_i
```
