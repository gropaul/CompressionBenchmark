#pragma once

#include "duckdb.hpp"
#include <cstdio>

#include "fsst.h"
#include "algorithms/api.hpp"
#include "models/compression_result.hpp"
#include "models/string_collection.hpp"
#include "models/benchmark_config.hpp"


inline CompressionResult RunFileExperiment(
    duckdb::Connection &con,
    const BenchmarkConfig &config, const TableConfig &file,
    const std::string &column_name
) {
    StringCollector collector(200000, 100000);

    std::ostringstream ss;
    ss << "SELECT " << column_name << " AS value "
            << "FROM "  << file.path
            << " LIMIT 100_000;\n";
    std::string query = ss.str();
    printf(query.c_str());
    const auto query_result = con.Query(query);
    printf("Done querying\n");
    if (query_result->HasError()) {
        printf(query_result->GetError().c_str());
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

    if (collector.Size() < 20000) {
        return CompressionResult{
            0,
        };
    }

    CompressionResult result(collector.TotalBytes());
    for (const CompressionAlgorithm algo: config.algorithms) {
        result.AddResult(Compress(algo, collector, config.n_repeats));
    }

    return result;
}


inline void RunExperiment(duckdb::Connection &con, const BenchmarkConfig &config) {
    for (const auto &file: config.tables) {
        for (const auto &column: file.columns) {
            auto res = RunFileExperiment(con, config, file, column);
            res.PrettyPrint();
        }
    }
}
