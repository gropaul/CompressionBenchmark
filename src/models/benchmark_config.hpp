
#pragma once


#include "compression_result.hpp"
#include "string_collection.hpp"


constexpr idx_t VECTOR_SIZE = 2048;
constexpr idx_t ROW_GROUP_SIZE_NUMBER_OF_BYTES = 4 * 256 * 1024; // we focus on blocks of compressed 256 KB, so 4x that uncompressed
constexpr idx_t ROW_GROUP_SIZE_NUMBER_OF_VALUES = 122880;
constexpr idx_t MIN_ROWS = ROW_GROUP_SIZE_NUMBER_OF_VALUES / 2;
constexpr idx_t MIN_NON_EMPTY_ROWS = ROW_GROUP_SIZE_NUMBER_OF_VALUES / 4;

constexpr idx_t N_RANDOM_ROW_ACCESSES = MIN_NON_EMPTY_ROWS;
constexpr idx_t N_RANDOM_VECTOR_ACCESSES = MIN_NON_EMPTY_ROWS / VECTOR_SIZE;

struct ExperimentState {
    size_t row_group_idx;
    size_t rows_offset;

    static ExperimentState Init() {
        return ExperimentState{0, 0};
    }
};


struct TableConfig {
    std::string name;
    std::vector<std::string> columns;
};

enum RowGroupMode {
    FIXED_NUMBER_OF_VALUES,
    FIXED_NUMBER_OF_BYTES
};

struct BenchmarkConfigMetaData {
    uint64_t n_repeats;
    uint64_t n_row_groups;
    bool filter_by_min_bytes;
    bool cut_by_min_bytes;
    std::vector<AlgorithType> algorithms;
    RowGroupMode row_group_mode;
};



struct BenchmarkConfig: BenchmarkConfigMetaData {
    std::vector<TableConfig> tables;
};


struct ExperimentInput {
    StringCollector &collector;
    std::vector<idx_t> random_row_indices;
    std::vector<idx_t> random_vector_indices;
};