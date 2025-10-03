#include <iostream>
#include <string>
#include "src/benchmarker.hpp"
#include "src/models/benchmark_config.hpp"
#include "src/models/compression_result.hpp"
#include "src/schema/config_creator.hpp"

void printUsage(const char* programName) {
    std::cout << "Usage: " << programName << " <duckdb_file> <output_csv>\n";
    std::cout << "  duckdb_file: Path to the DuckDB database file\n";
    std::cout << "  output_csv:  Path to the output CSV file\n";
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Error: Invalid number of arguments\n\n";
        printUsage(argv[0]);
        return 1;
    }

    const std::string duckdb_path = argv[1];
    const std::string output_csv = argv[2];

    try {
        duckdb::DuckDB db(duckdb_path);
        duckdb::Connection con(db);

        con.Query("PRAGMA threads=1");
        con.Query("SELECT version()")->GetValue(0,0).Print();

        const BenchmarkConfigMetaData meta = {
            5,
            10,
            {
                AlgorithType::FSST,
                AlgorithType::FSST12,
                AlgorithType::OnPair,
                AlgorithType::OnPair16,
                AlgorithType::OnPairMini10,
                AlgorithType::OnPairMini12,
                AlgorithType::OnPairMini14,
                AlgorithType::Dictionary
            }
        };
        const auto config = GetBenchmarkFromDatabase(con, meta);

        const auto results = RunExperiment(con, config);
        SaveResultsAsCSV(results, output_csv);

        std::cout << "Benchmark completed successfully. Results saved to: " << output_csv << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}