#pragma once

#include "duckdb.hpp"
#include <iomanip>

#include "algorithms/api.hpp"
#include "models/compression_result.hpp"
#include "models/string_collection.hpp"
#include "models/benchmark_config.hpp"


inline ExperimentResult RunFileExperiment(
    duckdb::Connection &con,
    const BenchmarkConfig &config, const TableConfig &table_config,
    const std::string &column_name
) {
    StringCollector collector(200000, ROW_GROUP_SIZE);

    std::ostringstream ss;
    ss << "SELECT " << column_name << " AS value "
            << "FROM "  << table_config.name
            << " LIMIT " << ROW_GROUP_SIZE;
    std::string query = ss.str();
    const auto query_result = con.Query(query);

    auto empty_result = ExperimentResult{
        0, 0, 0, table_config.name, column_name
    };

    if (query_result->HasError()) {
        printf("%s",query_result->GetError().c_str());
        return empty_result;
    } else {
        if (query_result->RowCount() < MIN_ROWS) {
            return empty_result;
        }
    }

    auto current_chunk = query_result->Fetch();
    while (current_chunk) {
        duckdb::Vector &strings_v = current_chunk->data[0];

        D_ASSERT(strings_v.GetType() == duckdb::LogicalType::VARCHAR);
        const auto strings = duckdb::FlatVector::GetData<duckdb::string_t>(strings_v);
        for (size_t idx = 0; idx < current_chunk->size(); idx++) {

            if (duckdb::FlatVector::IsNull(strings_v, idx)) {
                continue;
            }
            auto string_ddb = strings[idx];
            collector.AddStringDDB(string_ddb);
        }
        current_chunk = query_result->Fetch();
    }

    if (collector.Size() < MIN_NON_EMPTY_ROWS) {
        return empty_result;
    }

    ExperimentResult result(collector.TotalBytes(), query_result->RowCount(), collector.Size(), table_config.name, column_name);
    for (const AlgorithType algo: config.algorithms) {
        result.AddResult(Compress(algo, collector, config.n_repeats));
    }

    return result;
}


inline std::vector<ExperimentResult> RunExperiment(duckdb::Connection &con, const BenchmarkConfig &config) {
    std::vector<ExperimentResult> results;

    const uint64_t n_tables = config.tables.size();
    uint64_t current_table_index = 0;

    for (const auto &file: config.tables) {
        printf("Started table %llu of %llu: %s\n",
               static_cast<unsigned long long>(current_table_index),
               static_cast<unsigned long long>(n_tables),
               file.name.c_str());
        for (const auto &column: file.columns) {
            auto res = RunFileExperiment(con, config, file, column);
            results.push_back(res);
        }

        current_table_index += 1;
        printf("Finished table %llu of %llu: %s\n",
               static_cast<unsigned long long>(current_table_index),
               static_cast<unsigned long long>(n_tables),
               file.name.c_str());
    }
    return results;
}
