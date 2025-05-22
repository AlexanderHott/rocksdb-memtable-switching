import zmq
import optuna
from optuna.trial import Trial, FrozenTrial
from zmq import Socket
import logging
import pandas as pd
from datetime import datetime
import optuna.visualization as vis


class Workload:
    inserts: float = 0
    updates: float = 0
    point_deletes: float = 0
    range_deletes: float = 0
    point_queries: float = 0
    range_queries: float = 0
    insert_time: float
    update_time: float
    point_delete_time: float
    range_delete_time: float
    point_query_time: float
    range_query_time: float

    def __repr__(self) -> str:
        return (
            "Workload(\n"
            f"  I={self.inserts}, U={self.updates},\n"
            f"  PD={self.point_deletes}, RD={self.range_deletes},\n"
            f"  PQ={self.point_queries}, RQ={self.range_queries},\n"
            f"  I lat={self.insert_time}, U lat={self.update_time},\n"
            f"  PD lat={self.point_delete_time}, RD lat={self.range_delete_time},\n"
            f"  PQ lat={self.point_query_time}, RQ lat={self.range_query_time}\n"
            ")"
        )

    def __str__(self) -> str:
        return self.__repr__()

    @staticmethod
    def from_str(mission_str: str) -> "Workload":
        mission = Workload()
        logging.info(f"{mission_str=}")
        workload, latencies = mission_str.split(";")
        (mission.inserts,
         mission.updates,
         mission.point_deletes,
         mission.range_deletes,
         mission.point_queries,
         mission.range_queries) = map(float, workload.split(","))

        (mission.insert_time,
         mission.update_time,
         mission.point_delete_time,
         mission.range_delete_time,
         mission.point_query_time,
         mission.range_query_time) = map(float, latencies.split(":"))

        return mission

    def to_workload_percentages(self) -> tuple[float, float, float, float, float, float]:
        total_ops = self.inserts + self.updates + self.point_queries + self.range_queries + self.point_deletes + self.range_deletes
        return (
            round(self.inserts / total_ops, 1),
            round(self.updates / total_ops, 1),
            round(self.point_queries / total_ops, 1),
            round(self.range_queries / total_ops, 1),
            round(self.point_deletes / total_ops, 1),
            round(self.range_deletes / total_ops, 1),
        )

    def total_latency(self) -> float:
        return (
                self.insert_time +
                self.update_time +
                self.point_delete_time +
                self.range_delete_time +
                self.point_query_time +
                self.range_query_time
        )


class Objective:
    zmq_socket: Socket
    """IPC 'socket' for communicating with c++"""
    workload: Workload
    perf_metric: float
    name: str

    def __init__(self, zmq_socket: Socket):
        self.zmq_socket = zmq_socket

        # recv study name
        name_or_shutdown_str = self.zmq_socket.recv().decode("utf-8")
        if name_or_shutdown_str == "end":
            logging.info("shutting down")
            raise Exception("Got shutdown signal from C++")
        else:
            self.name = name_or_shutdown_str


        # recv workload
        workload_or_shutdown_str = self.zmq_socket.recv().decode("utf-8")
        if workload_or_shutdown_str == "end":
            logging.info("shutting down")
            raise Exception("Got shutdown signal from C++")
        logging.info(f"workload {workload_or_shutdown_str=}")
        workload = Workload.from_str(workload_or_shutdown_str)
        logging.info(workload)
        self.workload = workload

        self.perf_metric = workload.total_latency()
        logging.debug(f"{self.perf_metric=}")

    def __call__(self, trial: Trial) -> tuple[float, bool]:
        """Optimization function for optuna."""

        # set the same min and max to force the optimizer to "choose" the current workload
        # this is needed to let the optimizer know about the past workload without letting it dictate what it should be
        (inserts_pct, updates_pct, point_queries_pct, range_queries_pct, point_deletes_pct, range_deletes_pct) \
            = self.workload.to_workload_percentages()
        _insert = trial.suggest_float("inserts", inserts_pct, inserts_pct)
        _update = trial.suggest_float("updates", updates_pct, updates_pct)
        _delete_point = trial.suggest_float("point_deletes", point_deletes_pct, point_deletes_pct)
        _delete_range = trial.suggest_float("range_deletes", range_deletes_pct, range_deletes_pct)
        _query_point = trial.suggest_float("point_queries", point_queries_pct, point_queries_pct)
        _query_range = trial.suggest_float("range_queries", range_queries_pct, range_queries_pct)

        memtable = trial.suggest_categorical('memtable', [
            "vector",
            "skiplist",
            "hash-linklist",
            "hash-skiplist"
        ])

        memtable_config = ""
        memtable_size = trial.suggest_int("memtable_size", 24, 28)  # 2^16 = 64kb, 2^24=16mb
        memtable_config += str(2 ** memtable_size)
        # if memtable == "vector":

        logging.info(f"Suggesting {memtable=} with size 2^{memtable_size}")

        # send suggested memtable to c++
        self.zmq_socket.send_string(f"{memtable};{memtable_config}")

        mission_or_shutdown_str = self.zmq_socket.recv().decode("utf-8")
        if mission_or_shutdown_str == "end":
            logging.info("shutting down")
            return 0.0, True
        self.workload = Workload.from_str(mission_or_shutdown_str)
        if self.workload.insert_time < 0:
            print(mission_or_shutdown_str)
        # logging.info(self.mission)

        self.perf_metric = self.workload.total_latency()
        logging.debug(f"perf metric: {self.perf_metric}")

        return self.perf_metric, False


def run_workload():

    logging.basicConfig(level=logging.DEBUG)
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PAIR)
    ipc = "ipc:///tmp/rocksdb-memtable-switching-ipc"
    sock.connect(ipc)
    print(f"Listening on: {ipc}")

    try:
        objective = Objective(sock)
    except KeyboardInterrupt:
        exit(0)
    except Exception as e:
        logging.info(e)
        logging.exception(e)
        return
    study = optuna.create_study(direction="minimize", study_name=objective.name)

    # output = open("presentation/out.csv", "w")

    df = pd.read_csv("./data.csv")
    for i, (_, row) in enumerate(df.iterrows()):
        trial = optuna.trial.FrozenTrial(
            trial_id=i,
            number=i,
            value=row['value'],
            params={
                'inserts': row['params_inserts'],
                # 'updates': row['params_updates'],
                # 'point_deletes': row['params_point_deletes'],
                # 'range_deletes': row['params_range_deletes'],
                'point_queries': row['params_point_queries'],
                # 'range_queries': row['params_range_queries'],
                'memtable': row['params_memtable'],
                'memtable_size': row['params_memtable_size'],
            },
            distributions={
                'inserts': optuna.distributions.FloatDistribution(0., 1.),
                # 'updates': optuna.distributions.FloatDistribution(0., 1.),
                # 'point_deletes': optuna.distributions.FloatDistribution(0., 1.),
                # 'range_deletes': optuna.distributions.FloatDistribution(0., 1.),
                'point_queries': optuna.distributions.FloatDistribution(0., 1.),
                # 'range_queries': optuna.distributions.FloatDistribution(0., 1.),
                'memtable': optuna.distributions.CategoricalDistribution([
                    "vector",
                    "skiplist",
                    "hash-linklist",
                    "hash-skiplist"
                ]),
                'memtable_size': optuna.distributions.IntDistribution(24, 28),
            },
            user_attrs={},
            system_attrs={},
            intermediate_values={},
            datetime_start=datetime.now(),
            datetime_complete=datetime.now(),
            state=optuna.trial.TrialState.COMPLETE,
        )
        study.add_trial(trial)

    done = False
    while not done:
        trial = study.ask()
        ret, done = objective(trial)
        if done:
            logging.info(f"Ending study {study.study_name}")
            break
        study.tell(trial, ret)
        # output.write(f"{ret}\n")
        # output.flush()

    # output.close()
    sock.close()
    study.trials_dataframe().to_csv(f"{study.study_name}-results.csv")


if __name__ == "__main__":
    while True:
        run_workload()
