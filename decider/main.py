import zmq
import optuna
from optuna import Trial
from zmq import Socket
import logging


class Objective:
    zmq_socket: Socket
    """IPC 'socket' for communicating with c++"""
    last_time_ns: int
    """The last time a memtable was switched. Unix epoc in ns."""
    inserts: int
    """The number of inserts in the last workload."""
    point_queries: int
    """The number of point queries in the last workload."""

    def __init__(self, zmq_socket: Socket):
        self.zmq_socket = zmq_socket
        # get initial timestamp
        initial_time_ns_str = self.zmq_socket.recv().decode("utf-8")
        logging.debug(f"memtable flushed;{initial_time_ns_str}")

        try:
            initial_time_ns = int(initial_time_ns_str)
        except ValueError as e:
            print(f"Unable to parse initial time string as int {initial_time_ns_str}")
            raise ValueError from e

        self.last_time_ns = initial_time_ns

        # get initial workload
        initial_workload_str = self.zmq_socket.recv().decode("utf-8")
        logging.debug(f"workload;{initial_workload_str}")

        inserts_str, point_queries_str = initial_workload_str.split(",")
        self.inserts = int(inserts_str)
        self.point_queries = int(point_queries_str)

    def __call__(self, trial: Trial) -> int:
        """Optimization function for optuna."""
        # suggest new memtable
        total_opts = self.inserts + self.point_queries
        inserts_pct = self.inserts / total_opts
        point_queries_pct = self.point_queries / total_opts

        memtable = trial.suggest_categorical('memtable', ["vector", "skiplist", "hash-skiplist"])
        # set the same min and max to force the optimizer to "choose" the current workload
        # this is needed to let the optimizer know about the past workload without letting it dictate what it should be
        _insert = trial.suggest_float("inserts", inserts_pct, inserts_pct)
        _point_query = trial.suggest_float("point_queries", point_queries_pct, point_queries_pct)
        logging.info(f"Suggesting {memtable=} ({_insert=}, {_point_query=})")

        # send suggested memtable to c++
        self.zmq_socket.send_string(memtable)

        # receive workload from rocksdb
        workload_end_ns_str = self.zmq_socket.recv().decode("utf-8")
        workload_end_ns = int(workload_end_ns_str)

        logging.debug(f"memtable flushed;{workload_end_ns_str}")

        workload_str = self.zmq_socket.recv().decode("utf-8")
        logging.debug(f"workload;{workload_str}")
        inserts_str, point_queries_str = workload_str.split(",")
        self.inserts = int(inserts_str)
        self.point_queries = int(point_queries_str)

        total_workload_time = self.last_time_ns - workload_end_ns
        self.last_time_ns = workload_end_ns

        return total_workload_time


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PAIR)
    sock.connect("ipc:///tmp/rocksdb-memtable-switching-ipc")

    objective = Objective(sock)
    study = optuna.create_study()
    while True:
        trial = study.ask()
        ret = objective(trial)
        study.tell(trial, ret)
        # time = sock.recv()
        # print(f"Received: {time.decode('utf-8')}")
        #
        # workload_str = sock.recv().decode("utf-8")
        # print(f"{workload_str=}")
        #
        # sock.send_string("vectorrep from python")
