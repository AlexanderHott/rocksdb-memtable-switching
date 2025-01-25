#pragma once
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/json.hpp>

namespace cfg {
    using jsonns = nlohmann::json;

    struct CfgOpts {
        bool create_if_missing;
        bool allow_concurrent_memtable_write;
        std::string memtable_factory;
        size_t write_buffer_size;
        bool dynamic_memtable;
    };

    struct CfgWriteOpts {
    };

    struct CfgReadOpts {
    };

    struct CfgBlockBasedTableOpts {
    };

    struct CfgFlushOpts {
    };

    struct RocksdbOptions {
        rocksdb::Options opts;
        rocksdb::WriteOptions write_opts;
        rocksdb::ReadOptions read_opts;
        rocksdb::BlockBasedTableOptions table_opts;
        rocksdb::FlushOptions flush_opts;
    };

    class Cfg {
    public:
        CfgOpts opts;
        CfgWriteOpts write_opts;
        CfgReadOpts read_opts;
        CfgBlockBasedTableOpts table_opts;
        CfgFlushOpts flush_opts;

        static std::shared_ptr<RocksdbOptions> from_file(const std::string &filename);

    private:
        static std::optional<jsonns> read_json(const std::string &filename);

        [[nodiscard]] std::shared_ptr<RocksdbOptions> into_rocksdb() const;
    };
}