#pragma once

#include "duckdb.hpp"

#include "algorithms/api.hpp"
#include "models/compression_result.hpp"
#include "models/string_collection.hpp"
#include "models/benchmark_config.hpp"



inline void replace_all(std::string &str, const std::string &from, const std::string &to) {
    if(from.empty()) return;
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
}

inline ExperimentResult RunFileExperiment(
    duckdb::Connection &con,
    const BenchmarkConfig &config, const TableConfig &table_config,
    const std::string &column_name
) {
    StringCollector collector(200000, ROW_GROUP_SIZE_NUMBER_OF_VALUES);

    std::string query = R"(
        WITH numbered AS (
          SELECT
            row_number() OVER () AS rn,
            {{COLUMN_NAME}} as value,
            strlen(value) AS value_length
          FROM {{TABLE_NAME}}
          LIMIT {{ROW_GROUP_SIZE_NUMBER_OF_VALUES}}
        ),
        running_sum AS (
            SELECT
              rn,
              value,
              value_length,
              SUM(value_length) OVER (ORDER BY rn) AS running_sum
            FROM numbered
        )
        SELECT value FROM running_sum WHERE running_sum <= {{ROW_GROUP_SIZE_NUMBER_OF_BYTES}}
        )";

    // print the query
    // Replace placeholders
    replace_all(query, "{{COLUMN_NAME}}", column_name);
    replace_all(query, "{{TABLE_NAME}}", table_config.name);
    replace_all(query, "{{ROW_GROUP_SIZE_NUMBER_OF_VALUES}}", std::to_string(ROW_GROUP_SIZE_NUMBER_OF_VALUES));
    replace_all(query, "{{ROW_GROUP_SIZE_NUMBER_OF_BYTES}}", std::to_string(ROW_GROUP_SIZE_NUMBER_OF_BYTES));
    // std::cout << "Executing query:\n" << query << "\n";

    const auto query_result = con.Query(query);

    auto empty_result = ExperimentResult{
        0, 0, 0, 0, table_config.name, column_name
    };

    if (query_result->HasError()) {
        printf("%s", query_result->GetError().c_str());
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

    ExperimentResult result(0, collector.TotalBytes(), query_result->RowCount(),  collector.Size(), table_config.name,
                            column_name);

    const auto random_row_indices = GenerateRandomIndices(N_RANDOM_ROW_ACCESSES, collector.Size());
    const auto random_vector_indices = GenerateRandomIndices(N_RANDOM_VECTOR_ACCESSES, collector.Size() / VECTOR_SIZE);


    const ExperimentInput input{const_cast<StringCollector &>(collector), random_row_indices, random_vector_indices};

    for (const AlgorithType algo: config.algorithms) {
        result.AddResult(Compress(algo, input, config.n_repeats));
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
