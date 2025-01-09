#include "cfg.h"

#include <fstream>
#include <iostream>
#include <ostream>
#include <rocksdb/slice_transform.h>


#define LOG(msg) \
std::cout << __FILE__ << "(" << __LINE__ << "): " << msg << std::endl

namespace cfg {
    using json = nlohmann::json;

    void to_json(json &j, const CfgOpts &o) {
        j = json{{"create_if_missing", o.create_if_missing}};
        j = json{{"allow_concurrent_memtable_write", o.allow_concurrent_memtable_write}};
        j = json{{"memtable_factory", o.memtable_factory}};
        j = json{{"write_buffer_size", o.write_buffer_size}};
        j = json{{"dynamic_memtable", o.dynamic_memtable}};
    }

    void from_json(const json &j, CfgOpts &o) {
        j.at("create_if_missing").get_to(o.create_if_missing);
        j.at("allow_concurrent_memtable_write").get_to(o.allow_concurrent_memtable_write);
        j.at("memtable_factory").get_to(o.memtable_factory);
        j.at("write_buffer_size").get_to(o.write_buffer_size);
        j.at("dynamic_memtable").get_to(o.dynamic_memtable);
    }

    void to_json(json &j, const Cfg &c) {
        j = json{{"opts", c.opts}};
    }

    void from_json(const json &j, Cfg &c) {
        j.at("opts").get_to(c.opts);
    }

    std::shared_ptr<RocksdbOptions> Cfg::from_file(const std::string &filename) {
        std::string j = R"({"options": {"create_if_missing": true}})";
        const auto j_opt = read_json(filename);
        if (!j_opt) {
            LOG("Failed to read config file");
            return nullptr;
        }
        const auto &j_cfg = j_opt.value();
        const auto cfg = j_cfg.template get<Cfg>();

        return cfg.into_rocksdb();
    }

    std::optional<json> Cfg::read_json(const std::string &filename) {
        std::ifstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Could not open file: " << filename << std::endl;
            return std::nullopt;
        }

        try {
            json jsonData;
            file >> jsonData;
            return jsonData;
        } catch (const json::parse_error &e) {
            std::cerr << "JSON parse error: " << e.what() << std::endl;
            return std::nullopt;
        }
    }

    std::shared_ptr<RocksdbOptions> Cfg::into_rocksdb() const {
        auto rocksdb_opts = std::make_shared<RocksdbOptions>();
        rocksdb_opts->opts.create_if_missing = opts.create_if_missing;
        rocksdb_opts->opts.allow_concurrent_memtable_write = opts.allow_concurrent_memtable_write;
        rocksdb_opts->opts.write_buffer_size = opts.write_buffer_size;
        rocksdb_opts->opts.dynamic_memtable = opts.dynamic_memtable;

        if (opts.memtable_factory == "VectorRepFactory") {
            rocksdb_opts->opts.memtable_factory = std::make_shared<rocksdb::VectorRepFactory>();
        } else if (opts.memtable_factory == "SkipListFactory") {
            rocksdb_opts->opts.memtable_factory = std::make_shared<rocksdb::SkipListFactory>();
        } else if (opts.memtable_factory == "HashLinkListRepFactory") {
            rocksdb_opts->opts.memtable_factory.reset(
                rocksdb::NewHashLinkListRepFactory()
            );
            rocksdb_opts->opts.prefix_extractor.reset(
                rocksdb::NewFixedPrefixTransform(4)
            );
        } else if (opts.memtable_factory == "HashSkipListRepFactory") {
            rocksdb_opts->opts.memtable_factory.reset(
                rocksdb::NewHashSkipListRepFactory()
            );
            rocksdb_opts->opts.prefix_extractor.reset(
                rocksdb::NewFixedPrefixTransform(4)
            );
        } else {
            LOG("Unknown memtable factory: " << opts.memtable_factory);
            throw std::runtime_error("Unknown memtable factory");
        }
        rocksdb_opts->opts.prefix_extractor.reset(
            rocksdb::NewFixedPrefixTransform(4));

        return rocksdb_opts;
    }
}
