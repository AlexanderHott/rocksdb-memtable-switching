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
    Insert,
    Update,
    PointDelete,
    RangeDelete,
    PointQuery,
    RangeQuery,
};
class SlidingWindow {
public:
    explicit SlidingWindow(const size_t size) : maxSize(size) {}

    void add(const DBOperation item) {
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
        counts[item]++;
    }
    std::string getCountsAsPercentages() const {
        const auto totalItems = static_cast<double>(window.size());
        if (totalItems == 0) {
            return "No items in the window.";
        }

        std::stringstream ss;
        ss << std::fixed << std::setprecision(4); // Set precision for percentage

        for (const auto& [category, count] : counts) {
            const double percentage = static_cast<double>(count) / totalItems * 100.0;
            ss << percentage << ",";
        }

        return ss.str();
    }

private:
    std::deque<DBOperation> window;
    std::unordered_map<DBOperation, size_t> counts;
    size_t maxSize;
};

void memtable_decider(rocksdb::DB *db, const SlidingWindow& workload_stats) {

  // zmq_context = zmq::context_t();
  // zmq_socket = zmq::socket_t(zmq_context, zmq::socket_type::pair);
  // zmq_socket.bind("ipc:///tmp/rocksdb-memtable-switching-ipc");
    for (int i = 0; i < 10; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(3));
        db->memtable_factory_mutex_.lock();
        LOG((typeid(db->memtable_factory_.get()) == typeid(SkipListFactory) ? "true" : "false"));
        if (typeid(*db->memtable_factory_) == typeid(SkipListFactory)) {
            db->memtable_factory_ = std::make_shared<VectorRepFactory>();
        } else {
            db->memtable_factory_ = std::make_shared<SkipListFactory>();
        }
        LOG(db->memtable_factory_->Name());
        db->memtable_factory_mutex_.unlock();
        LOG(workload_stats.getCountsAsPercentages());
    }
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

    SlidingWindow workload_stats(1000);

    // std::string line;
    // while (std::getline(*input, line)) {
    //     switch (line[0]) {
    //         case 'I':
    //             break;
    //     }
    //     std::cout << line << std::endl;
    // }

    // found via looking at the "memtable seal time;" log
    const auto TIMES = 145570;
    LOG("Start inserts");

    db->memtable_factory_mutex_.lock();
    db->memtable_factory_ = std::make_shared<SkipListFactory>();
    db->memtable_factory_mutex_.unlock();
    std::thread decider_thread([&db, &workload_stats] {
        memtable_decider(db, workload_stats);
    });
    decider_thread.detach();

    // if (env->IsPerfIOStatEnabled()) {
    //     SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    //     get_perf_context()->Reset();
    //     get_perf_context()->ClearPerLevelPerfContext();
    //     get_perf_context()->EnablePerLevelPerfContext();
    //     get_iostats_context()->Reset();
    // }
    for (int t = 0; t < 10; ++t) {
        LOG("===" << t << "===");
        LOG("Insert start Time;" << duration_cast<ns>(hrc::now().time_since_epoch()).count());
        for (size_t i = 0; i < TIMES; ++i) {
            // if (i > 14500) db->memtable_impl = "vector";
            // if (i % 10 == 0) std::this_thread::sleep_for(std::chrono::milliseconds(10));
            std::string key = "k" + std::to_string(i) + std::string(5, '0');
            std::string val = std::to_string(i) + std::string(5, '0') + std::string(10, 'v');
            db->Put(w_options, key.substr(0, 5), val.substr(0, 11));
            workload_stats.add(DBOperation::Insert);
        }
        std::string val;
        for (size_t i = 0; i < 1000; ++i) {
            std::string key = "k" + std::to_string(i) + std::string(5, '0');
            db->Get(r_options, key.substr(0, 5), &val);
            workload_stats.add(DBOperation::PointQuery);
        }
        LOG("insert end Time;" << duration_cast<ns>(hrc::now().time_since_epoch()).count());
        LOG("flush write bytes " << db->GetOptions().statistics->getTickerCount(FLUSH_WRITE_BYTES));
        LOG("memtable bytes at flush " << db->GetOptions().statistics->getTickerCount(MEMTABLE_PAYLOAD_BYTES_AT_FLUSH));
    }
    // if (env->IsPerfIOStatEnabled()) {
    //     rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    //     std::cout << "RocksDB Perf Context : " << std::endl;
    //     std::cout << rocksdb::get_perf_context()->ToString() << std::endl;
    //     std::cout << "RocksDB Iostats Context : " << std::endl;
    //     std::cout << rocksdb::get_iostats_context()->ToString() << std::endl;
    // }

    delete db;
    rocksdb::DestroyDB(db_path, options);
}
