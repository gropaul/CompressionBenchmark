

#include "duckdb.hpp"
#include <string>

#include "../models/benchmark_config.hpp"

inline BenchmarkConfig GetBenchmarkFromDatabase(duckdb::Connection &con, BenchmarkConfigMetaData meta) {
    auto result = con.Query(
        "SELECT table_schema, table_name, column_name "
        "FROM information_schema.columns "
        "WHERE data_type = 'VARCHAR'");

    std::unordered_map<std::string, TableConfig> table_map;

    for (idx_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
        auto table_schema  = result->GetValue(0, row_idx).ToString();
        auto table_name  = result->GetValue(1, row_idx).ToString();
        auto column_name = result->GetValue(2, row_idx).ToString();

        const auto full_table_name = "\"" + table_schema + "\".\"" + table_name + "\"";
        const auto full_column_name = "\"" + column_name + "\"";

        auto &tbl = table_map[full_table_name];
        if (tbl.name.empty()) {
            tbl.name = full_table_name;
        }
        tbl.columns.push_back(full_column_name);
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
