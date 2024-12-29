#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <thread>

#include "db_env.h"
#include "config_options.h"

#include <zmq.hpp>

using hrc = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;
using std::chrono::duration_cast;

constexpr long MISSION_SIZE = 5000;

#define LOG(msg) \
  std::cout << __FILE__ << "(" << __LINE__ << "): " << msg << std::endl

class FlushEventListener final : public rocksdb::EventListener {
public:
    explicit FlushEventListener(std::atomic_bool &did_flush): did_flush_(did_flush) {
    }

    void OnMemTableSealed(const rocksdb::MemTableInfo &mem_table_info) override {
        did_flush_.store(true, std::memory_order_seq_cst);
        LOG("memtable sealed time;"
            << duration_cast<ns>(hrc::now().time_since_epoch()).count()
            << " "
            << mem_table_info.num_entries
            << " "
            << did_flush_.load(std::memory_order_seq_cst));
    }

private:
    std::atomic_bool &did_flush_;
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

void joinVector(std::stringstream &ss, const std::vector<long>& vec, const std::string& delimiter) {
    for (size_t i = 0; i < vec.size(); ++i) {
        if (i != 0) {
            ss << delimiter;
        }
        ss << vec[i];
    }
}

/**
 * Only one timer can be running at a time. E.g. you can only call `start()` once before a call to `end(...)`.
 */
class StatsCollector {
public:
    void start() {
        start_ = hrc::now();
    }

    void end(DBOperation op) {
        const auto now = hrc::now();
        const auto duration = duration_cast<ns>(now - start_).count();
        // Make sure to lock the mutex after measuring e2e time, so that lock acquisition isn't considered in the
        // duration.
        std::lock_guard guard(mutex_);
        switch (op) {
            case DBOperation::Insert: {
                inserts_.push_back(duration);
                break;
            }
            case DBOperation::Update: {
                updates_.push_back(duration);
                break;
            }
            case DBOperation::PointDelete: {
                point_deletes_.push_back(duration);
                break;
            }
            case DBOperation::RangeDelete: {
                range_deletes_.push_back(duration);
                break;
            }
            case DBOperation::PointQuery: {
                point_queries_.push_back(duration);
                break;
            }
            case DBOperation::RangeQuery: {
                range_queries_.push_back(duration);
            }
            default: {
                LOG("ERROR: unhandled db operation in StatsCollector::end: " << op);
            }
        }
    }

    [[nodiscard]] size_t size() {
        std::lock_guard guard(mutex_);
        return inserts_.size() + updates_.size() + point_deletes_.size() + range_deletes_.size() +
               point_queries_.size() + range_queries_.size();
    }

    std::string mission_results() {
        LOG("generating mission results");
        const size_t total_items = size();

        LOG("took lock");
        std::stringstream ss;
        ss << std::fixed << std::setprecision(4);
        LOG("sending workload makeup");
        mutex_.lock();
        ss << static_cast<double>(inserts_.size()) / total_items * 100.0 << ",";
        ss << static_cast<double>(updates_.size()) / total_items * 100.0 << ",";
        ss << static_cast<double>(point_deletes_.size()) / total_items * 100.0 << ",";
        ss << static_cast<double>(range_deletes_.size()) / total_items * 100.0 << ",";
        ss << static_cast<double>(point_queries_.size()) / total_items * 100.0 << ",";
        ss << static_cast<double>(range_queries_.size()) / total_items * 100.0 << ";";

        LOG("sending avg latencies");
        joinVector(ss, inserts_, ",");
        ss << ":";
        joinVector(ss, updates_, ",");
        ss << ":";
        joinVector(ss, point_deletes_, ",");
        ss << ":";
        joinVector(ss, range_deletes_, ",");
        ss << ":";
        joinVector(ss, point_queries_, ",");
        ss << ":";
        joinVector(ss, range_queries_, ",");
        // ss << (inserts_.empty()
        //            ? 0
        //            : std::accumulate(inserts_.begin(), inserts_.end(), 0L) / static_cast<long double>(inserts_.size()))
        //         << ",";
        // ss << (updates_.empty() ? 0 : std::accumulate(updates_.begin(), updates_.end(), 0L) / updates_.size()) << ",";
        // ss << (point_deletes_.empty()
        //            ? 0
        //            : std::accumulate(point_deletes_.begin(), point_deletes_.end(), 0L) / point_deletes_.size()) << ",";
        // ss << (range_deletes_.empty()
        //            ? 0
        //            : std::accumulate(range_deletes_.begin(), range_deletes_.end(), 0L) / range_deletes_.size()) << ",";
        // ss << (point_queries_.empty()
        //            ? 0
        //            : std::accumulate(point_queries_.begin(), point_queries_.end(), 0L) / point_queries_.size()) << ",";
        // ss << (range_queries_.empty()
        //            ? 0
        //            : std::accumulate(range_queries_.begin(), range_queries_.end(), 0L) / range_queries_.size());


        inserts_.clear();
        updates_.clear();
        point_deletes_.clear();
        range_deletes_.clear();
        point_queries_.clear();
        range_queries_.clear();

        LOG("generated workload");

        mutex_.unlock();
        return ss.str();
    }

private:
    std::mutex mutex_;
    std::chrono::time_point<hrc> start_;
    std::vector<long> inserts_;
    std::vector<long> updates_;
    std::vector<long> point_deletes_;
    std::vector<long> range_deletes_;
    std::vector<long> point_queries_;
    std::vector<long> range_queries_;
};

// To start the deciding process, we send a syn and wait for an ack
// We use a condvar to let the main thread "sleep" while
// the decider thread waits for the ack from python.
std::mutex start_flag_mutex;
std::condition_variable start_cv;
bool start_flag = false;
// To shut down the decider thread, we use an atomic bool that
// gets checked in a while loop in the decider thread
// and gets set to true when the workload is complete.
std::atomic_bool stop_flag(false);

void memtable_decider(rocksdb::DB *db, StatsCollector &stats_collector, std::atomic_bool &did_flush) {
    zmq::context_t zmq_context;
    zmq::socket_t zmq_socket(zmq_context, zmq::socket_type::pair);
    zmq_socket.bind("ipc:///tmp/rocksdb-memtable-switching-ipc");

    // SYN ACK with decider
    LOG("[DECIDER]: Sending syn");
    std::string syn_str = "syn";
    zmq::message_t syn_msg(syn_str.data(), syn_str.size());
    zmq_socket.send(syn_msg, zmq::send_flags::none);
    zmq::message_t ack_msg;
    const zmq::recv_result_t ack_resp = zmq_socket.recv(ack_msg, zmq::recv_flags::none);
    assert(ack_resp.has_value());
    // scope for lock guard
    {
        std::lock_guard lock(start_flag_mutex);
        start_flag = true;
    }
    start_cv.notify_one();

    while (!stop_flag.load(std::memory_order_relaxed)) {
        if (stats_collector.size() < MISSION_SIZE || !did_flush.load(std::memory_order_seq_cst)) {
            LOG("[DECIDER]: sleep " << stats_collector.size() << " < " << MISSION_SIZE << " " << !did_flush.load(std::
                memory_order_seq_cst));
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        // std::this_thread::sleep_for(std::chrono::seconds(30));
        LOG("[DECIDER]: iteration");
        std::string mission_str = stats_collector.mission_results();
        LOG("[DECIDER]: got mission results " << mission_str);
        zmq::message_t mission_msg(mission_str.data(), mission_str.size());
        zmq_socket.send(mission_msg, zmq::send_flags::none);

        zmq::message_t memtable_msg;
        auto n = zmq_socket.recv(memtable_msg, zmq::recv_flags::none);
        if (!n.has_value()) {
            LOG("[DECIDER]: error no bytes read from python");
        }
        std::string memtable_str(static_cast<char *>(memtable_msg.data()), memtable_msg.size());

        LOG("[DECIDER]: chose " << memtable_str);
        db->memtable_factory_mutex_.lock();
        const size_t semi_idx = memtable_str.find(';');
        assert(semi_idx != std::string::npos);
        std::string memtable_impl = memtable_str.substr(0, semi_idx);

        if (memtable_impl == "vector") {
            const int memtable_size = std::stoi(memtable_str.substr(semi_idx + 1));
            db->memtable_factory_ = std::make_shared<VectorRepFactory>(std::pow(2, memtable_size));
            const std::unordered_map<std::string, std::string> new_db_opts = {
                {"write_buffer_size", std::to_string(memtable_size)}
            };
            db->SetDBOptions(new_db_opts);
            // db->memtable_factory_ = std::make_shared<VectorRepFactory>(std::pow(2, 20));
        } else if (memtable_impl == "skiplist") {
            db->memtable_factory_ = std::make_shared<SkipListFactory>();
        } else if (memtable_impl == "hash-linklist") {
            db->memtable_factory_.reset(NewHashLinkListRepFactory());
        } else if (memtable_impl == "hash-skiplist") {
            db->memtable_factory_.reset(NewHashSkipListRepFactory());
        } else {
            LOG("[DECIDER]: error: invalid memtable str: " << memtable_impl);
        }
        LOG("[DECIDER]: " << db->memtable_factory_->Name() << " initialized");
        db->memtable_factory_mutex_.unlock();
        did_flush.store(false, std::memory_order_seq_cst);
    }

    LOG("[DECIDER]: shutting down");
    const std::string shutdown_str = "shutdown";
    zmq::message_t shutdown_msg(shutdown_str.data(), shutdown_str.size());
    zmq_socket.send(shutdown_msg, zmq::send_flags::none);
    zmq_socket.close();
    LOG("[DECIDER]: sent shutdown signal");
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

    std::atomic_bool did_flush = false;
    options.listeners.push_back(std::make_shared<FlushEventListener>(did_flush));
    // options.memtable_factory = std::make_shared<VectorRepFactory>(env->vector_preallocation_size_in_bytes);
    options.memtable_factory = std::make_shared<SkipListFactory>();
    // options.memtable_factory.reset(NewHashLinkListRepFactory(
    //     env->bucket_count, env->linklist_huge_page_tlb_size,
    //     env->linklist_bucket_entries_logging_threshold,
    //     env->linklist_if_log_bucket_dist_when_flash,
    //     env->linklist_threshold_use_skiplist)
    //     );
    // options.memtable_factory.reset(NewHashSkipListRepFactory());
    // options.prefix_extractor.reset(
    //     NewFixedPrefixTransform(env->prefix_length));

    std::string db_path = "/tmp/rocksdb-memtable-switching";

    rocksdb::DestroyDB(db_path, options);

    rocksdb::Status s;
    s = rocksdb::DB::Open(options, db_path, &db);
    if (!s.ok())
        LOG(s.ToString());

    StatsCollector stats_collector;

    db->memtable_factory_mutex_.lock();
    db->memtable_factory_ = options.memtable_factory;
    db->memtable_factory_mutex_.unlock();
    std::thread decider_thread([&db, &stats_collector,&did_flush] {
        memtable_decider(db, stats_collector, did_flush);
    });
    // new scope for lock guard
    {
        LOG("waiting on decider thread");
        std::unique_lock lock(start_flag_mutex);
        start_cv.wait(lock, [] { return start_flag; });
    }

    for (auto path: {
             "1m_i.txt",
             // "500k_i-500k_pq.txt",
             // "990k_i-10k_pq.txt"
         }) {
        LOG("RUNNING WORKLOAD " << path);
        std::istream *input;
        std::ifstream file;
        file.open("../workload-gen/workloads/" + std::string(path));
        input = &file;

        std::string line;
        while (std::getline(*input, line)) {
            switch (line[0]) {
                case 'I': {
                    size_t i_sp = line.find(' ', 2);
                    stats_collector.start();
                    db->Put(w_options, line.substr(2, i_sp - 2), line.substr(i_sp + 1));
                    stats_collector.end(DBOperation::Insert);
                    break;
                }
                case 'U': {
                    size_t u_sp = line.find(' ', 2);
                    stats_collector.start();
                    db->Put(w_options, line.substr(2, u_sp - 2), line.substr(u_sp + 1));
                    stats_collector.end(DBOperation::Update);
                    break;
                }
                case 'P': {
                    std::string val;
                    stats_collector.start();
                    db->Get(r_options, line.substr(2), &val);
                    stats_collector.end(DBOperation::PointQuery);
                    break;
                }
                case 'R': {
                    // Range Query
                    size_t rq_sp = line.find(' ', 2);
                    rocksdb::Iterator *it = db->NewIterator(r_options);
                    std::string rq_k_beg = line.substr(2, rq_sp - 2);
                    std::string rq_k_end = line.substr(rq_sp + 1);
                    stats_collector.start();
                    for (
                        it->Seek(rq_k_beg);
                        it->Valid() && it->key().ToString() < rq_k_end;
                        it->Next()
                    ) {
                        auto _ = it->value();
                    }
                    stats_collector.end(DBOperation::RangeQuery);
                    break;
                }
                case 'D': {
                    stats_collector.start();
                    db->Delete(w_options, line.substr(2));
                    stats_collector.end(DBOperation::PointDelete);
                    break;
                }
                case 'X': {
                    // Range Delete
                    size_t rd_sp = line.find(' ', 2);
                    stats_collector.start();
                    db->DeleteRange(w_options, line.substr(2, rd_sp - 2), line.substr(rd_sp + 1));
                    stats_collector.end(DBOperation::RangeDelete);
                    break;
                }
                default:
                    LOG("ERROR: unknown operation in workload: " << line[0]);
            }
        }
    }

    LOG("Storing stop flag");
    stop_flag.store(true, std::memory_order_relaxed);
    LOG("joining decider");
    decider_thread.join();
    LOG("joined decider, ending");


    delete db;
    // rocksdb::DestroyDB(db_path, options);
}
