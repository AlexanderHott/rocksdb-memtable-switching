# Rocksdb Memtable Switching

## Usage

### Cloning

[Git docs on submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules)

```bash
git clone git@github.com:AlexanderHott/rocksdb-memtable-switching.git --recurse-submodules
```

### Running

For running each individual project, check their README.

1. Generate a workload with `workload-gen-cli`
2. Run the python decider script
3. Run the rocksdb benchmarking program with the generated workload

## TODO

- [ ] check for newer hyperparameter optimization library similar to optuna
- [ ] hook up workload generator
- [ ] make db_env / rocksdb options configurable via json

## Docs

In rocksdb, the memtable choice has a large impact on many different performance characteristics: throughput, operation 
latency, write amplification, etc... The workload and memtable implementation pair also plays a large role in determining the 
performance, a small (1%) change in workload can make the memtable 10x-100x slower. 

This project experiments with a dynamic way of switching memtable implementations depending on the current workload.

### Problem statement

Given a workload, what is the best memtable implementation and configuration to optimize for a certain performance 
metric.

### Solution

This problem can be modeled as a classic function optimization problem $f(workload) -> performance$. From this, I 
decided to use the python library optuna that implements TPE+CMA-ES, a better version of GP-BO. 

A small issue with modeling the function arises when trying to the objective function. I have to define the space that
the optimization model is able to suggest a new vector (list of inputs) to run the database with. Since the workload is
technically part of the inputs to this function, the model needs to take it into account for creating "correlations" 
between workload and memtable implementation we need a way of restricting it to its actual value. The ML model does not 
dictate the workload, the user does. I solved this by specifying the same start and end value for the range that the
model is allowed to choose a value.
```py
#                                        V start      V end
_insert = trial.suggest_float("inserts", inserts_pct, inserts_pct)
_point_query = trial.suggest_float("point_queries", point_queries_pct, point_queries_pct)
```
Optuna was the only library that I found that was able to change the bounds of the space on a per-run basis. I could 
have written the optimization algorithm from scratch, but that is not the focus of this paper.

TODO: Optimization loop visualization

---

Since rocksdb is not written in python, I need a way for c++ and python to communicate. I thought of using files, shared 
memory, or FFI, but all of those require significant implementation for creating a communication channel. I decided to 
use the zmq family of libraries. For the "ipc" option, the use unix domain sockets (or named pipes on windows). It also
handles the message sending and receiving logic.

```py
initial_workload_str = self.zmq_socket.recv().decode("utf-8")
```
