

#include "duckdb.hpp"
#include <string>

#include "../models/benchmark_config.hpp"

inline BenchmarkConfig GetBenchmarkFromDatabase(duckdb::Connection &con, BenchmarkConfigMetaData meta) {
    auto result = con.Query("SELECT table_name, column_name FROM information_schema.columns WHERE data_type = 'VARCHAR'");

    std::unordered_map<std::string, TableConfig> table_map;

    for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
        auto table_name  = result->GetValue(0, row_idx).ToString();
        auto column_name = result->GetValue(1, row_idx).ToString();

        auto &tbl = table_map[table_name];
        if (tbl.name.empty()) {
            tbl.name = table_name;
        }
        tbl.columns.push_back(column_name);
    }

    std::vector<TableConfig> configs;
    configs.reserve(table_map.size());
    for (auto &kv : table_map) {
        configs.push_back(std::move(kv.second));
    }

    return BenchmarkConfig{
        meta,
        configs
    };
}
