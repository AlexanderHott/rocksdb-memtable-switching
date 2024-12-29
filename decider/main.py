import zmq
import optuna
from optuna.trial import Trial, FrozenTrial
from zmq import Socket
import logging
import pandas as pd
from datetime import datetime


class Mission:
    inserts: float = 0
    updates: float = 0
    point_deletes: float = 0
    range_deletes: float = 0
    point_queries: float = 0
    range_queries: float = 0
    insert_time: list[int]
    update_time: list[int]
    point_delete_time: list[int]
    range_delete_time: list[int]
    point_query_time: list[int]
    range_query_time: list[int]

    def __repr__(self) -> str:
        return (
            "Mission(\n"
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
    def from_str(mission_str: str) -> "Mission":
        mission = Mission()
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
         mission.range_query_time) = map(
            lambda lst: map(int, lst.split(",")),
            latencies.split(":")
        )
        # turn "1,2,3:2,3,4" into [1,2,3], [2,3,4]

        return mission

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
    mission: Mission
    perf_metric: float

    def __init__(self, zmq_socket: Socket):
        self.zmq_socket = zmq_socket

        # recv workload
        mission_or_shutdown_str = self.zmq_socket.recv().decode("utf-8")
        if mission_or_shutdown_str == "shutdown":
            logging.info("shutting down")
            exit(0)
        logging.info(f"mission {mission_or_shutdown_str=}")
        mission = Mission.from_str(mission_or_shutdown_str)
        logging.info(mission)
        self.mission = mission

        self.perf_metric = mission.total_latency()
        logging.debug(f"{self.perf_metric=}")

    def __call__(self, trial: Trial) -> tuple[float, bool]:
        """Optimization function for optuna."""

        # set the same min and max to force the optimizer to "choose" the current workload
        # this is needed to let the optimizer know about the past workload without letting it dictate what it should be
        (inserts_pct, updates_pct, point_queries_pct, range_queries_pct, point_deletes_pct, range_deletes_pct) \
            = self.mission.to_percentages()
        _insert = trial.suggest_float("inserts", inserts_pct, inserts_pct)
        _update = trial.suggest_float("updates", updates_pct, updates_pct)
        _delete_point = trial.suggest_float("point_deletes", point_deletes_pct, point_queries_pct)
        _delete_range = trial.suggest_float("range_deletes", range_deletes_pct, range_deletes_pct)
        _query_point = trial.suggest_float("point_queries", point_queries_pct, point_queries_pct)
        _query_range = trial.suggest_float("range_queries", range_queries_pct, range_queries_pct)

        memtable = trial.suggest_categorical('memtable', [
            "vector",
            # "skiplist",
            # "hash-linklist",
            # "hash-skiplist"
        ])

        memtable_config = ""
        if memtable == "vector":
            memtable_size = trial.suggest_int("memtable_size", 24, 24)  # 2^16 = 64kb, 2^24=16mb
            memtable_config += str(memtable_size)

        # logging.info(f"Suggesting {memtable=} with size 2^{memtable_size} ({_insert=}, {_query_point=})")

        # send suggested memtable to c++
        self.zmq_socket.send_string(f"{memtable};{memtable_config}")

        mission_or_shutdown_str = self.zmq_socket.recv().decode("utf-8")
        if mission_or_shutdown_str == "shutdown":
            logging.info("shutting down")
            return 0.0, True
        self.mission = Mission.from_str(mission_or_shutdown_str)
        if self.mission.insert_time < 0:
            print(mission_or_shutdown_str)
        # logging.info(self.mission)

        self.perf_metric = self.mission.total_latency()
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
    study = optuna.create_study(direction="minimize")

    output = open("presentation/out.csv", "w")

    # df = pd.read_csv("./data.csv")
    # for i, (_, row) in enumerate(df.iterrows()):
    #     trial = optuna.trial.FrozenTrial(
    #         trial_id=i,
    #         number=i,
    #         value=row['value'],
    #         params={
    #             'inserts': row['params_inserts'],
    #             'point_queries': row['params_point_queries'],
    #             'memtable': row['params_memtable'],
    #         },
    #         distributions={
    #             'inserts': optuna.distributions.FloatDistribution(0., 1.),
    #             'point_queries': optuna.distributions.FloatDistribution(0., 1.),
    #             'memtable': optuna.distributions.CategoricalDistribution([
    #                 "vector",
    #                 "skiplist",
    #                 "hash-linklist",
    #                 "hash-skiplist"
    #             ])
    #         },
    #         user_attrs={},
    #         system_attrs={},
    #         intermediate_values={},
    #         datetime_start=datetime.now(),
    #         datetime_complete=datetime.now(),
    #         state=optuna.trial.TrialState.COMPLETE,
    #     )
    #     study.add_trial(trial)

    done = False
    while not done:
        trial = study.ask()
        ret, done = objective(trial)
        study.tell(trial, ret)
        output.write(f"{ret}\n")
        output.flush()

    output.close()
    study.trials_dataframe().to_csv(f"{study.study_name}-results.csv")
