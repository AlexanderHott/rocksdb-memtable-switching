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
    point_queries: float = 0
    range_queries: float = 0
    point_deletes: float = 0
    range_deletes: float = 0

    def __init__(self):
        pass

    def __repr__(self) -> str:
        return f"Workload({self.inserts}, {self.updates}, {self.point_queries}, {self.range_queries}, {self.point_deletes}, {self.range_deletes})"

    def __str__(self) -> str:
        return self.__repr__()

    @staticmethod
    def from_str(workload_str: str) -> "Workload":
        op_counts = workload_str[:-1].split(",")
        workload = Workload()
        for op_count in op_counts:
            op, count_str = op_count.split(":")
            count = round(float(count_str), 1)
            match op:
                case "Insert":
                    workload.inserts = count
                case "Update":
                    workload.updates = count
                case "PointDelete":
                    workload.point_deletes = count
                case "RangeDelete":
                    workload.range_deletes = count
                case "PointQuery":
                    workload.point_queries = count
                case "RangeQuery":
                    workload.range_queries = count
                case _:
                    raise ValueError(f"Invalid operation {op}")

        return workload

    def to_percentages(self) -> tuple[float, float, float, float, float, float]:
        total_ops = self.inserts + self.updates + self.point_queries + self.range_queries + self.point_deletes + self.range_deletes
        return (
            round(self.inserts / total_ops, 1),
            round(self.updates / total_ops, 1),
            round(self.point_queries / total_ops, 1),
            round(self.range_queries / total_ops, 1),
            round(self.point_deletes / total_ops, 1),
            round(self.range_deletes / total_ops, 1),
        )


class Objective:
    zmq_socket: Socket
    """IPC 'socket' for communicating with c++"""
    workload: Workload
    perf_metric: float

    def __init__(self, zmq_socket: Socket):
        self.zmq_socket = zmq_socket

        # recv workload
        workload_or_shutdown_str = self.zmq_socket.recv().decode("utf-8")
        if workload_or_shutdown_str == "shutdown":
            logging.info("shutting down")
            exit(0)
        logging.info(workload_or_shutdown_str)
        wk = Workload.from_str(workload_or_shutdown_str)
        logging.info(wk)
        self.workload = wk
        logging.debug(f"{self.workload=}")

        perf_metric_str = self.zmq_socket.recv().decode("utf-8")
        self.perf_metric = float(perf_metric_str)
        logging.debug(f"perf metric: {self.perf_metric}")

    def __call__(self, trial: Trial) -> tuple[float, bool]:
        """Optimization function for optuna."""

        # set the same min and max to force the optimizer to "choose" the current workload
        # this is needed to let the optimizer know about the past workload without letting it dictate what it should be
        (inserts_pct, updates_pct, point_queries_pct, range_queries_pct, point_deletes_pct, range_deletes_pct) \
            = self.workload.to_percentages()
        _insert = trial.suggest_float("inserts", inserts_pct, inserts_pct)
        _point_query = trial.suggest_float("point_queries", point_queries_pct, point_queries_pct)

        memtable = trial.suggest_categorical('memtable', [
            "vector",
            "skiplist",
            "hash-linklist",
            "hash-skiplist"
        ])

        logging.info(f"Suggesting {memtable=} ({_insert=}, {_point_query=})")

        # send suggested memtable to c++
        self.zmq_socket.send_string(memtable)

        workload_or_shutdown_str = self.zmq_socket.recv().decode("utf-8")
        if workload_or_shutdown_str == "shutdown":
            logging.info("shutting down")
            return 0.0, True
        self.workload = Workload.from_str(workload_or_shutdown_str)

        perf_metric_str = self.zmq_socket.recv().decode("utf-8")
        self.perf_metric = float(perf_metric_str)
        logging.debug(f"perf metric: {self.perf_metric}")

        return self.perf_metric, False


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PAIR)
    ipc = "ipc:///tmp/rocksdb-memtable-switching-ipc"
    sock.connect(ipc)
    print(f"Listening on: {ipc}")

    print("Waiting on syn")
    syn = sock.recv().decode("utf-8")
    sock.send_string("ack")
    print("Starting to optimize")

    objective = Objective(sock)
    study = optuna.create_study(direction="maximize")

    df = pd.read_csv("./data.csv")
    for i, (_, row) in enumerate(df.iterrows()):
        trial = optuna.trial.FrozenTrial(
            trial_id=i,
            number=i,
            value=row['value'],
            params={
                'inserts': row['params_inserts'],
                'point_queries': row['params_point_queries'],
                'memtable': row['params_memtable'],
            },
            distributions={
                'inserts': optuna.distributions.FloatDistribution(0., 1.),
                'point_queries': optuna.distributions.FloatDistribution(0., 1.),
                'memtable': optuna.distributions.CategoricalDistribution([
                    "vector",
                    "skiplist",
                    "hash-linklist",
                    "hash-skiplist"
                ])
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
        study.tell(trial, ret)

    study.trials_dataframe().to_csv(f"{study.study_name}-results.csv")


