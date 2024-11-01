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

- change key types
  - string, int
- point queries based on existing or non existing keys
- range delete impl
- different distributions