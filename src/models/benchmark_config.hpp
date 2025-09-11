
#pragma once;

struct TableConfig {
    std::string name;
    std::vector<std::string> columns;
};


struct BenchmarkConfigMetaData {
    uint64_t n_repeats;
    std::vector<CompressionAlgorithm> algorithms;
};

struct BenchmarkConfig: BenchmarkConfigMetaData {

    std::vector<TableConfig> tables;
};