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
#include <filesystem>  

using hrc = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;
using std::chrono::duration_cast;

#define LOG(msg) \
  std::cout << __FILE__ << "(" << __LINE__ << "): " << msg << std::endl

class FlushEventListener : public rocksdb::EventListener {
    void OnMemTableSealed(const rocksdb::MemTableInfo &mem_table_info) override {
        LOG("memtable sealed time;"
            << duration_cast<ns>(hrc::now().time_since_epoch()).count()
            << " "
            << mem_table_info.num_entries);
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
    zmq_socket.recv(ack_msg, zmq::recv_flags::none); {
        std::lock_guard lock(start_flag_mutex);
        start_flag = true;
    }
    start_cv.notify_one();

    while (!stop_flag.load(std::memory_order_relaxed)) {
        LOG("DECIDER iteration");
        std::this_thread::sleep_for(std::chrono::seconds(30));
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

        LOG("Chose: " << memtable_str);
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
        LOG("[Decider] " << db->memtable_factory_->Name());
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
    // options.memtable_factory = std::make_shared<VectorRepFactory>(env->vector_preallocation_size_in_bytes);
    // options.memtable_factory = std::make_shared<SkipListFactory>();
    // options.memtable_factory.reset(NewHashLinkListRepFactory(
    //     env->bucket_count, env->linklist_huge_page_tlb_size,
    //     env->linklist_bucket_entries_logging_threshold,
    //     env->linklist_if_log_bucket_dist_when_flash,
    //     env->linklist_threshold_use_skiplist)
    //     );
    options.memtable_factory.reset(NewHashSkipListRepFactory());
    options.prefix_extractor.reset(
        NewFixedPrefixTransform(env->prefix_length));

    std::string db_path = "/tmp/rocksdb-memtable-switching";

    rocksdb::DestroyDB(db_path, options);

    rocksdb::Status s;
    s = rocksdb::DB::Open(options, db_path, &db);
    if (!s.ok())
        LOG(s.ToString());

    SlidingWindow workload_stats(100000);


    db->memtable_factory_mutex_.lock();
    db->memtable_factory_ = options.memtable_factory;
    db->memtable_factory_mutex_.unlock();
    std::thread decider_thread([&db, &workload_stats] {
        memtable_decider(db, workload_stats);
    }); {
        LOG("waiting on decider thread");
        std::unique_lock lock(start_flag_mutex);
        start_cv.wait(lock, [] { return start_flag; });
    }

    // for (auto path: {"1ki_1kpq.txt", "1m_i.txt"}) {
    //     LOG("RUNNING WORKLOAD " << path);
    //     for (int i = 0; i < 50; ++i) {
    //         std::istream *input;
    //         std::ifstream file;
    //         file.open("../workload-gen/workloads/" + std::string(path));
    //         input = &file;

    //         std::string line;
    //         while (std::getline(*input, line)) {
    //             switch (line[0]) {
    //                 case 'I': {
    //                     size_t i_sp = line.find(' ', 2);
    //                     db->Put(w_options, line.substr(2, i_sp - 2), line.substr(i_sp + 1));
    //                     workload_stats.add(DBOperation::Insert);
    //                     break;
    //                 }
    //                 case 'U': {
    //                     size_t u_sp = line.find(' ', 2);
    //                     db->Put(w_options, line.substr(2, u_sp - 2), line.substr(u_sp + 1));
    //                     workload_stats.add(DBOperation::Update);
    //                     break;
    //                 }
    //                 case 'P': {
    //                     std::string val;
    //                     db->Get(r_options, line.substr(2), &val);
    //                     workload_stats.add(DBOperation::PointQuery);
    //                     break;
    //                 }
    //                 case 'R': {
    //                     // Range Query
    //                     size_t rq_sp = line.find(' ', 2);
    //                     rocksdb::Iterator *it = db->NewIterator(r_options);
    //                     std::string rq_k_beg = line.substr(2, rq_sp - 2);
    //                     std::string rq_k_end = line.substr(rq_sp + 1);
    //                     for (
    //                         it->Seek(rq_k_beg);
    //                         it->Valid() && it->key().ToString() < rq_k_end;
    //                         it->Next()
    //                     ) {
    //                         auto _ = it->value();
    //                     }
    //                     workload_stats.add(DBOperation::RangeQuery);
    //                     break;
    //                 }
    //                 case 'D': {
    //                     db->Delete(w_options, line.substr(2));
    //                     workload_stats.add(DBOperation::PointDelete);
    //                     break;
    //                 }
    //                 case 'X': {
    //                     // Range Delete
    //                     size_t rd_sp = line.find(' ', 2);
    //                     db->DeleteRange(w_options, line.substr(2, rd_sp - 2), line.substr(rd_sp + 1));
    //                     workload_stats.add(DBOperation::RangeDelete);
    //                     break;
    //                 }
    //                 default:
    //                     LOG("ERROR: unknown operation: " << line[0]);
    //             }
    //         }
    //     }
    // }

    // iterate through all workload files in the directory rather than hardcode file name
    namespace fs = std::filesystem;
    std::string workload_dir = "../workload-gen/workloads";

    for (const auto& entry : fs::directory_iterator(workload_dir)) {
        if (entry.is_regular_file()) {
            std::string path = entry.path().string();
            LOG("RUNNING WORKLOAD " << path);

            for (int i = 0; i < 50; ++i) {
                std::istream *input;
                std::ifstream file;
                file.open(path);
                if (!file.is_open()) {
                    LOG("ERROR: Could not open file " << path);
                    continue;
                }
                input = &file;

                std::string line;
                while (std::getline(*input, line)) {
                    switch (line[0]) {
                        case 'I': {
                            size_t i_sp = line.find(' ', 2);
                            db->Put(w_options, line.substr(2, i_sp - 2), line.substr(i_sp + 1));
                            workload_stats.add(DBOperation::Insert);
                            break;
                        }
                        case 'U': {
                            size_t u_sp = line.find(' ', 2);
                            db->Put(w_options, line.substr(2, u_sp - 2), line.substr(u_sp + 1));
                            workload_stats.add(DBOperation::Update);
                            break;
                        }
                        case 'P': {
                            std::string val;
                            db->Get(r_options, line.substr(2), &val);
                            workload_stats.add(DBOperation::PointQuery);
                            break;
                        }
                        case 'R': {
                            // Range Query
                            size_t rq_sp = line.find(' ', 2);
                            rocksdb::Iterator *it = db->NewIterator(r_options);
                            std::string rq_k_beg = line.substr(2, rq_sp - 2);
                            std::string rq_k_end = line.substr(rq_sp + 1);
                            for (it->Seek(rq_k_beg); it->Valid() && it->key().ToString() < rq_k_end; it->Next()) {
                                auto _ = it->value();
                            }
                            workload_stats.add(DBOperation::RangeQuery);
                            break;
                        }
                        case 'D': {
                            db->Delete(w_options, line.substr(2));
                            workload_stats.add(DBOperation::PointDelete);
                            break;
                        }
                        case 'X': {
                            // Range Delete
                            size_t rd_sp = line.find(' ', 2);
                            db->DeleteRange(w_options, line.substr(2, rd_sp - 2), line.substr(rd_sp + 1));
                            workload_stats.add(DBOperation::RangeDelete);
                            break;
                        }
                        default:
                            LOG("ERROR: unknown operation: " << line[0]);
                    }
                }
            }
        }
    }

    LOG("Storing stop flag");
    stop_flag.store(true, std::memory_order_relaxed);
    LOG("joinging decider");
    decider_thread.join();
    LOG("joined decider, ending");


    delete db;
    // rocksdb::DestroyDB(db_path, options);
}
