#pragma once

#include "duckdb.hpp"

#include "algorithms/api.hpp"
#include "models/compression_result.hpp"
#include "models/string_collection.hpp"
#include "models/benchmark_config.hpp"


inline void replace_all(std::string &str, const std::string &from, const std::string &to) {
    if (from.empty()) return;
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
}

inline ExperimentResult RunExperimentForColumn(
    duckdb::Connection &con,
    const BenchmarkConfig &config,
    const TableConfig &table_config,
    const std::string &column_name,
    const ExperimentState &state
) {
    StringCollector collector(ROW_GROUP_SIZE_NUMBER_OF_BYTES, ROW_GROUP_SIZE_NUMBER_OF_VALUES);

    // check if we have enough data in the table:
    // a) at least MIN_ROWS rows in total
    // b) OR at least ROW_GROUP_SIZE_NUMBER_OF_BYTES bytes of data
    std::string check_query = R"(
        SELECT
            ifnull(COUNT(*) = {{ROW_GROUP_SIZE_NUMBER_OF_VALUES}}, false) AS has_enough_rows,
            ifnull(SUM(strlen({{COLUMN_NAME}})) >= {{ROW_GROUP_SIZE_NUMBER_OF_BYTES}}, false) AS has_enough_bytes
        FROM (
            FROM {{TABLE_NAME}}
            LIMIT {{ROW_GROUP_SIZE_NUMBER_OF_VALUES}}
            OFFSET {{ROWS_OFFSET}}
        )
        )";
    replace_all(check_query, "{{COLUMN_NAME}}", column_name);
    replace_all(check_query, "{{TABLE_NAME}}", table_config.name);
    replace_all(check_query, "{{ROW_GROUP_SIZE_NUMBER_OF_VALUES}}", std::to_string(ROW_GROUP_SIZE_NUMBER_OF_VALUES));
    replace_all(check_query, "{{ROW_GROUP_SIZE_NUMBER_OF_BYTES}}", std::to_string(ROW_GROUP_SIZE_NUMBER_OF_BYTES));
    replace_all(check_query, "{{ROWS_OFFSET}}", std::to_string(state.rows_offset));

    const auto check_query_result = con.Query(check_query);
    // printf("Query: %s\n", check_query.c_str());
    if (check_query_result->HasError()) {
        printf("%s\n", check_query_result->GetError().c_str());
    }
    const auto has_enough_rows = check_query_result->GetValue(0, 0).GetValue<bool>();
    auto has_enough_bytes = check_query_result->GetValue(1, 0).GetValue<bool>();

    if (!config.filter_by_min_bytes) {
        has_enough_bytes = true;
    }

    if (!has_enough_rows && !has_enough_bytes) {
        return ExperimentResult::Empty();
    }

    std::string where_clause = "";
    if (config.cut_by_min_bytes) {
        where_clause = "WHERE running_sum <= " + std::to_string(ROW_GROUP_SIZE_NUMBER_OF_BYTES);
    }
    std::string query = R"(
        WITH numbered AS (
          SELECT
            row_number() OVER () AS rn,
            {{COLUMN_NAME}} as value,
            strlen(value) AS value_length
          FROM (
            FROM {{TABLE_NAME}}
            LIMIT {{ROW_GROUP_SIZE_NUMBER_OF_VALUES}}
            OFFSET {{ROWS_OFFSET}}
          )
        ),
        running_sum AS (
            SELECT
              rn,
              value,
              value_length,
              SUM(value_length) OVER (ORDER BY rn) AS running_sum
            FROM numbered
        )
        SELECT value FROM running_sum {{WHERE_CLAUSE}}
        )";

    // print the query
    // Replace placeholders
    replace_all(query, "{{COLUMN_NAME}}", column_name);
    replace_all(query, "{{TABLE_NAME}}", table_config.name);
    replace_all(query, "{{ROW_GROUP_SIZE_NUMBER_OF_VALUES}}", std::to_string(ROW_GROUP_SIZE_NUMBER_OF_VALUES));
    replace_all(query, "{{ROW_GROUP_SIZE_NUMBER_OF_BYTES}}", std::to_string(ROW_GROUP_SIZE_NUMBER_OF_BYTES));
    replace_all(query, "{{ROWS_OFFSET}}", std::to_string(state.rows_offset));
    replace_all(query, "{{WHERE_CLAUSE}}", where_clause);
    // std::cout << "Executing query:\n" << query << "\n";

    const auto query_result = con.Query(query);

    if (query_result->HasError() || query_result->RowCount() == 0) {
        printf("%s", query_result->GetError().c_str());
        return ExperimentResult::Empty();
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

    if (collector.Size() == 0) {
        return ExperimentResult::Empty();
    }

    printf("Running experiment for table %s, column %s, row group %llu: collected %llu rows, %llu bytes (lengths: %llu bytes)\n",
           table_config.name.c_str(), column_name.c_str(),
           static_cast<unsigned long long>(state.row_group_idx),
           static_cast<unsigned long long>(collector.Size()),
           static_cast<unsigned long long>(collector.TotalBytes()),
           static_cast<unsigned long long>(collector.TotalSizeLengths())
    );

    ExperimentResult result(state.rows_offset, state.row_group_idx,
                            collector.TotalSizeRequired(),
                            collector.TotalBytes(), collector.TotalSizeLengths(),
                            query_result->RowCount(), collector.Size(),
                            table_config.name, column_name
    );

    const auto random_row_indices = GenerateRandomIndices(N_RANDOM_ROW_ACCESSES, collector.Size() - 1);
    const auto random_vector_indices = GenerateRandomIndices(N_RANDOM_VECTOR_ACCESSES, (collector.Size() / VECTOR_SIZE) - 1);

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
            auto state = ExperimentState::Init();
            idx_t row_group_idx = 0;
            while (row_group_idx < config.n_row_groups) {
                auto res = RunExperimentForColumn(con, config, file, column, state);
                results.push_back(res);
                if (res.GetNumRows() == 0) {
                    break;
                }
                state.row_group_idx += 1;
                state.rows_offset += res.GetNumRows();
                row_group_idx += 1;
            }
        }

        current_table_index += 1;
        printf("Finished table %llu of %llu: %s\n",
               static_cast<unsigned long long>(current_table_index),
               static_cast<unsigned long long>(n_tables),
               file.name.c_str());
    }
    return results;
}
