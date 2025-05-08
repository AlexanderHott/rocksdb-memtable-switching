#include <fstream>
#include <iostream>
#include <filesystem>
#include <string>
#include <rocksdb/db.h>

#include <zmq.hpp>
#include <stats_collector.hpp>

#include "cfg.h"

using hrc = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;
using std::chrono::duration_cast;
using json = nlohmann::json;
namespace fs = std::filesystem;

#define LOG(msg) \
std::cout << __FILE__ << "(" << __LINE__ << "): " << msg << std::endl


std::string sanitize_file_name(const std::string &file_path) {
    const fs::path path(file_path);
    const std::unordered_set invalid_chars = {'<', '>', ':', '"', '/', '\\', '|', '?', '*'};
    const std::unordered_set remove_chars = {'.', ' '};

    std::string sanitized = path.stem().string();

    std::replace_if(sanitized.begin(), sanitized.end(),
                    [&](const char c) { return invalid_chars.find(c) != invalid_chars.end(); },
                    '-'
    );

    sanitized.erase(
        std::remove_if(
            sanitized.begin(),
            sanitized.end(),
            [&](const char c) { return remove_chars.find(c) != remove_chars.end(); }
        ),
        sanitized.end()
    );

    return sanitized;
}


// class FlushEventListener final : public rocksdb::EventListener {
// public:
//     explicit FlushEventListener() = default;
//
//     void OnMemTableSealed(const rocksdb::MemTableInfo &mem_table_info) override {
//         LOG("memtable sealed with "
//             << mem_table_info.num_entries
//             << " entries"
//         );
//     }
// };


void benchmark(const std::string &config_path, const std::string &workload_path, const std::string &save_path) {
    auto cfg = cfg::Cfg::from_file(config_path);
    auto [opts, write_opts, read_opts, table_opts, flush_opts] = *cfg;

    std::string db_path = "/tmp/rocksdb-memtable-switching";

    rocksdb::DB *db;
    DestroyDB(db_path, opts);

    // opts.listeners.push_back(std::make_shared<FlushEventListener>());

    rocksdb::Status s;
    s = rocksdb::DB::Open(opts, db_path, &db);
    if (!s.ok()) {
        LOG(s.ToString());
        return;
    }

    // LOG("Setting write_buffer_size to 524288");
    // db->SetOptions({
    //         {"write_buffer_size", "524288"},
    //     }
    // );

    if (opts.dynamic_memtable) {
        std::string study_name =
                sanitize_file_name(workload_path) + "--" +
                sanitize_file_name(config_path);
        zmq::message_t msg(study_name.data(), study_name.size());
        db->zmq_socket_->send(msg, zmq::send_flags::none);
    }

    db->stats_collector_ = std::make_shared<StatsCollector>();
    // db->zmq_context_ = zmq::context_t();
    // db->zmq_socket_ = std::make_shared<zmq::socket_t>(db->zmq_context_, zmq::socket_type::pair);
    // db->zmq_socket_->bind("ipc:///tmp/rocksdb-memtable-switching-ipc");

    LOG("running workload " << workload_path << " with config " << config_path);
    std::istream *input;
    std::ifstream file;
    file.open(std::string(workload_path));
    input = &file;

    std::string line;
    while (std::getline(*input, line)) {
        switch (line[0]) {
            case 'I': {
                size_t i_sp = line.find(' ', 2);
                db->stats_collector_->start();
                db->Put(write_opts, line.substr(2, i_sp - 2), line.substr(i_sp + 1));
                db->stats_collector_->end(OpType::kInsert);
                break;
            }
            case 'U': {
                size_t u_sp = line.find(' ', 2);
                db->stats_collector_->start();
                db->Put(write_opts, line.substr(2, u_sp - 2), line.substr(u_sp + 1));
                db->stats_collector_->end(OpType::kUpdate);
                break;
            }
            case 'P': {
                std::string val;
                db->stats_collector_->start();
                db->Get(read_opts, line.substr(2), &val);
                db->stats_collector_->end(OpType::kQueryPoint);
                break;
            }
            case 'R': {
                // Range Query
                size_t rq_sp = line.find(' ', 2);
                rocksdb::Iterator *it = db->NewIterator(read_opts);
                std::string rq_k_beg = line.substr(2, rq_sp - 2);
                std::string rq_k_end = line.substr(rq_sp + 1);
                db->stats_collector_->start();
                for (
                    it->Seek(rq_k_beg);
                    it->Valid() && it->key().ToString() < rq_k_end;
                    it->Next()
                ) {
                    auto _ = it->value();
                }
                db->stats_collector_->end(OpType::kQueryRange);
                break;
            }
            case 'D': {
                db->stats_collector_->start();
                db->Delete(write_opts, line.substr(2));
                db->stats_collector_->end(OpType::kDeletePoint);
                break;
            }
            case 'X': {
                // Range Delete
                size_t rd_sp = line.find(' ', 2);
                db->stats_collector_->start();
                db->DeleteRange(write_opts, line.substr(2, rd_sp - 2), line.substr(rd_sp + 1));
                db->stats_collector_->end(OpType::kDeleteRange);
                break;
            }
            default:
                LOG("ERROR: unknown operation in workload: " << line[0]);
        }
    }

    const auto save_path_dir = fs::path(save_path);
    const auto save_path_file = fs::path(
        sanitize_file_name(workload_path) + "--" +
        sanitize_file_name(config_path) + ".results.json"
    );
    LOG("Writing results to file " << save_path_file.string());
    db->stats_collector_->write_to_file(save_path_dir / save_path_file);

    if (opts.dynamic_memtable) {
        std::string end_string = "end";
        zmq::message_t msg(end_string.data(), end_string.size());
        db->zmq_socket_->send(msg, zmq::send_flags::none);
        db->zmq_socket_->close();
    }
    delete db;
}

bool ends_with(const std::string &str, const std::string &suffix) {
    return str.size() >= suffix.size() &&
           str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        LOG("Usage: " << argv[0] << " <workload_run_path>");
        return 1;
    }
    std::string workload_run_path = argv[1];
    if (!fs::exists(workload_run_path)) {
        LOG("workload_run_path " << workload_run_path << " does not exist");
        return 1;
    }
    if (!fs::is_directory(workload_run_path)) {
        LOG("workload_run_path " << workload_run_path << " is not a directory");
        return 1;
    }

    std::vector<std::string> configs;
    std::vector<std::string> workloads;

    for (const auto &entry: fs::directory_iterator(workload_run_path)) {
        const std::string path = entry.path().string();
        const std::string filename = entry.path().filename().string();

        if (ends_with(filename, ".txt")) {
            workloads.push_back(path);
        } else if (ends_with(filename, ".options.json")) {
            configs.push_back(path);
        }
    }

    LOG("Workloads");
    for (const auto &path: workloads) {
        LOG("  " << path);
    }
    LOG("Configs");
    for (const auto &path: configs) {
        LOG("  " << path);
    }

    for (const auto &config: configs) {
        for (const auto &workload: workloads) {
            if (
                (workload == "../benchmark-runs/dynamic/5k_i-445k_pq.txt" &&
                 config == "../benchmark-runs/dynamic/vector.options.json")
                ||
                (workload == "../benchmark-runs/dynamic/250k_i-250k_pq.txt" &&
                 config == "../benchmark-runs/dynamic/vector.options.json")
                ||
                (workload == "../benchmark-runs/dynamic/dynamic.txt" &&
                 config == "../benchmark-runs/dynamic/vector.options.json")
            ) {
                LOG("Skipping slow workload");
                continue;
            }
            benchmark(
                config,
                workload,
                workload_run_path
            );
        }
    }

    return 0;
}
