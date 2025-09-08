
#pragma once;

struct TableConfig {
    std::string path;
    std::vector<std::string> columns;
};


struct BenchmarkConfigMetaData {
    uint64_t n_repeats;
    std::vector<CompressionAlgorithm> algorithms;
};

struct BenchmarkConfig: BenchmarkConfigMetaData {

    std::vector<TableConfig> tables;
};