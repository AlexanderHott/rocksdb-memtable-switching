#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <variant>

#include "json.hpp"

using hrc = std::chrono::high_resolution_clock;
using time_point = std::chrono::time_point<hrc>;
using ns = std::chrono::nanoseconds;
using std::chrono::duration_cast;
namespace fs = std::filesystem;


enum OpType :unsigned char {
    kInsert,
    kUpdate,
    kDelete,
    kQueryPoint,
    kQueryRange,
    kDeletePoint,
    kDeleteRange,
};

static_assert(sizeof(OpType) == 1);

NLOHMANN_JSON_SERIALIZE_ENUM(OpType, {
                             {OpType::kInsert, "Insert"},
                             {OpType::kUpdate, "Update"},
                             {OpType::kDeletePoint, "PointDelete"},
                             {OpType::kDeleteRange, "RangeDelete"},
                             {OpType::kQueryPoint, "PointQuery"},
                             {OpType::kQueryRange, "RangeQuery"}
                             })

struct MemtableSwitchEvent {
    std::string memtable;
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(MemtableSwitchEvent, memtable);

struct OperationCompleteEvent {
    long duration;
    OpType opType;
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(OperationCompleteEvent, duration, opType);

using Event = std::variant<MemtableSwitchEvent, OperationCompleteEvent>;

using json = nlohmann::json;

inline void to_json(json &j, const Event &event) {
    if (std::holds_alternative<MemtableSwitchEvent>(event)) {
        j = json{{"type", "MemtableSwitchEvent"}, {"data", std::get<MemtableSwitchEvent>(event)}};
    } else if (std::holds_alternative<OperationCompleteEvent>(event)) {
        j = json{{"type", "OperationCompleteEvent"}, {"data", std::get<OperationCompleteEvent>(event)}};
    }
}

inline void from_json(const json &j, Event &event) {
    const auto &type = j.at("type").get<std::string>();
    if (type == "MemtableSwitchEvent") {
        event = j.at("data").get<MemtableSwitchEvent>();
    } else if (type == "OperationCompleteEvent") {
        event = j.at("data").get<OperationCompleteEvent>();
    } else {
        throw std::invalid_argument("Invalid BenchmarkEvent type");
    }
}


class StatsCollector {
public:
    StatsCollector()
        : start_(hrc::now()) {
    }

    void start() {
        std::lock_guard guard(this->mutex_);
        start_ = hrc::now();
    }

    void end(const OpType opType) {
        std::lock_guard guard(this->mutex_);
        const auto duration = hrc::now() - start_;
        events_.emplace_back(OperationCompleteEvent{duration.count(), opType});
    }

    void write_to_file(const fs::path &filepath) {
        std::lock_guard guard(this->mutex_);
        std::ofstream output(filepath);

        if (!output) {
            std::cerr << "Error: Could not open file " << filepath << " for writing.\n";
        }
        const json j = events_;
        output << j.dump(2);
        output.close();
    }

private:
    time_point start_;
    std::vector<Event> events_{};
    std::mutex mutex_{};
};
