#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include <chrono>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

#include "db_env.h"
#include "config_options.h"

#include <zmq.hpp>

using hrc = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;
using std::chrono::duration_cast;

#define LOG(msg) \
  std::cout << __FILE__ << "(" << __LINE__ << "): " << msg << std::endl

class FlushEventListener : public rocksdb::EventListener {
    void OnMemTableSealed(const rocksdb::MemTableInfo &mem_table_info) override {
        // LOG("memtable sealed time;" << duration_cast<ns>(hrc::now().time_since_epoch()).count() << " " << mem_table_info
        // .
        // num_entries);
    }
};

enum class DBOperation {
    Insert = 0,
    Update,
    PointQuery,
    RangeQuery,
    PointDelete,
    RangeDelete,
};

std::ostream &operator<<(std::ostream &os, DBOperation op) {
    switch (op) {
        case DBOperation::Insert: os << "Insert";
            break;
        case DBOperation::Update: os << "Update";
            break;
        case DBOperation::PointDelete: os << "PointDelete";
            break;
        case DBOperation::RangeDelete: os << "RangeDelete";
            break;
        case DBOperation::PointQuery: os << "PointQuery";
            break;
        case DBOperation::RangeQuery: os << "RangeQuery";
            break;
    }
    return os;
}

class SlidingWindow {
public:
    explicit SlidingWindow(const size_t size) : maxSize(size), op_count(0) {
    }

    void add(const DBOperation item) {
        this->mutex.lock();
        ++this->op_count;

        if (window.size() == maxSize) {
            // Remove oldest item
            const DBOperation oldest = window.front();
            counts[oldest]--;
            if (counts[oldest] == 0) {
                counts.erase(oldest);
            }
            window.pop_front();
        }
        // Add new item
        window.push_back(item);
        this->mutex.unlock();
        counts[item]++;
    }

    std::optional<std::string> getCountsAsPercentages() {
        this->mutex.lock();
        const auto totalItems = static_cast<double>(window.size());
        if (totalItems == 0) {
            this->mutex.unlock();
            return std::nullopt;
        }

        std::stringstream ss;
        ss << std::fixed << std::setprecision(4); // Set precision for percentage

        for (const auto &[category, count]: counts) {
            const double percentage = static_cast<double>(count) / totalItems * 100.0;
            LOG(category << ":" << percentage);
            ss << category << ":" << percentage << ",";
        }
        this->mutex.unlock();
        return ss.str();
    }

    size_t throughput() {
        std::lock_guard lock(mutex);
        const size_t ret = this->op_count;
        this->op_count = 0;
        return ret;
    }

private:
    std::mutex mutex;
    std::deque<DBOperation> window;
    std::unordered_map<DBOperation, size_t> counts;
    size_t maxSize;

    size_t op_count;
};

// To start the deciding process, we send a syn and wait for an ack
// We use a condvar to let the main thread "sleep" while
// the decider thread waits for the ack from python.
std::mutex start_flag_mutex;
std::condition_variable start_cv;
bool start_flag = false;
// To shutdown the decider thread, we use an atomic bool that
// gets checked in a while loop in the decider thread
// and gets set to true when the workload is complete.
std::atomic_bool stop_flag(false);

void memtable_decider(rocksdb::DB *db, SlidingWindow &workload_stats) {
    zmq::context_t zmq_context;
    zmq::socket_t zmq_socket(zmq_context, zmq::socket_type::pair);
    zmq_socket.bind("ipc:///tmp/rocksdb-memtable-switching-ipc");

    // SYN ACK with decider
    LOG("Sending syn");
    std::string syn_str = "syn";
    zmq::message_t syn_msg(syn_str.data(), syn_str.size());
    zmq_socket.send(syn_msg, zmq::send_flags::none);
    zmq::message_t ack_msg;
    zmq_socket.recv(ack_msg, zmq::recv_flags::none);

    {
        std::lock_guard lock(start_flag_mutex);
        start_flag = true;
    }
    start_cv.notify_one();

    while (!stop_flag.load(std::memory_order_relaxed)) {
        LOG("DECIDER iteration");
        std::this_thread::sleep_for(std::chrono::seconds(15));
        std::optional<std::string> workload_str_opt = workload_stats.getCountsAsPercentages();
        if (!workload_str_opt.has_value()) {
            continue;
        }

        std::string workload_str = workload_str_opt.value();
        zmq::message_t workload_msg(workload_str.data(), workload_str.size());
        zmq_socket.send(workload_msg, zmq::send_flags::none);

        std::string perf_metric_str(std::to_string(workload_stats.throughput()));
        zmq::message_t perf_metric_msg(perf_metric_str.data(), perf_metric_str.size());
        zmq_socket.send(perf_metric_msg, zmq::send_flags::none);

        zmq::message_t memtable_msg;
        auto n = zmq_socket.recv(memtable_msg, zmq::recv_flags::none);
        if (!n.has_value()) {
            LOG("error no bytes read");
        }
        std::string memtable_str(static_cast<char *>(memtable_msg.data()), memtable_msg.size());

        db->memtable_factory_mutex_.lock();
        if (memtable_str == "vector") {
            db->memtable_factory_ = std::make_shared<VectorRepFactory>();
        } else if (memtable_str == "skiplist") {
            db->memtable_factory_ = std::make_shared<SkipListFactory>();
        } else if (memtable_str == "hash-linklist") {
            db->memtable_factory_.reset(NewHashLinkListRepFactory());
        } else if (memtable_str == "hash-skiplist") {
            db->memtable_factory_.reset(NewHashSkipListRepFactory());
        } else {
            LOG("ERROR: invalid memtable str: " << memtable_str);
        }
        // LOG(db->memtable_factory_->Name());
        db->memtable_factory_mutex_.unlock();
    }

    LOG("Shutting down decider");
    std::string shutdown_str = "shutdown";
    zmq::message_t shutdown_msg(shutdown_str.data(), shutdown_str.size());
    zmq_socket.send(shutdown_msg, zmq::send_flags::none);
    zmq_socket.close();
    LOG("sent shutdown signal");
}

int main(int argc, char *argv[]) {
    // std::istream* input;
    // std::ifstream file;
    //
    // if (argc > 1) {
    //     // Open the file passed as the first argument
    //     file.open(argv[1]);
    //     if (!file.is_open()) {
    //         std::cerr << "Error: Could not open file " << argv[1] << std::endl;
    //         return 1;
    //     }
    //     input = &file;
    // } else {
    //     // No file passed, use standard input
    //     input = &std::cin;
    // }

    DBEnv *env = DBEnv::GetInstance();

    DB *db;
    Options options;
    WriteOptions w_options;
    ReadOptions r_options;
    BlockBasedTableOptions table_options;
    FlushOptions f_options;

    configOptions(env, &options, &table_options, &w_options, &r_options,
                  &f_options);

    options.listeners.push_back(std::make_shared<FlushEventListener>());
    std::string db_path = "/tmp/rocksdb-memtable-switching";

    rocksdb::DestroyDB(db_path, options);

    rocksdb::Status s;
    s = rocksdb::DB::Open(options, db_path, &db);
    if (!s.ok())
        LOG(s.ToString());

    SlidingWindow workload_stats(100000);

    // std::string line;
    // while (std::getline(*input, line)) {
    //     switch (line[0]) {
    //         case 'I':
    //             break;
    //     }
    //     std::cout << line << std::endl;
    // }

    const auto TIMES = 5000;
    LOG("Start inserts");

    db->memtable_factory_mutex_.lock();
    db->memtable_factory_ = std::make_shared<SkipListFactory>();
    db->memtable_factory_mutex_.unlock();
    std::thread decider_thread([&db, &workload_stats] {
        memtable_decider(db, workload_stats);
    }); {
        LOG("waiting on decider thread");
        std::unique_lock lock(start_flag_mutex);
        start_cv.wait(lock, [] { return start_flag; });
    }

    for (int t = 0; t < 5000; ++t) {
        LOG("===" << t << "===");
        for (size_t i = 0; i < TIMES; ++i) {
            // if (i > 14500) db->memtable_impl = "vector";
            // if (i % 10 == 0) std::this_thread::sleep_for(std::chrono::milliseconds(10));
            std::string key = "k" + std::to_string(i) + std::string(5, '0');
            std::string val = std::to_string(i) + std::string(5, '0') + std::string(10, 'v');
            db->Put(w_options, key.substr(0, 5), val.substr(0, 11));
            workload_stats.add(DBOperation::Insert);
        }
        // std::string val;
        // for (size_t i = 0; i < TIMES / 10; ++i) {
        //     std::string key = "k" + std::to_string(i) + std::string(5, '0');
        //     db->Get(r_options, key.substr(0, 5), &val);
        //     workload_stats.add(DBOperation::PointQuery);
        // }
    }

    LOG("Storing stop flag");
    stop_flag.store(true, std::memory_order_relaxed);
    LOG("joinging decider");
    decider_thread.join();
    LOG("joined decider, ending");


    delete db;
    // rocksdb::DestroyDB(db_path, options);
}
