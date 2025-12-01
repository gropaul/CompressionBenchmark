#include <iostream>
#include <string>
#include <vector>
#include "src/benchmarker.hpp"
#include "src/models/benchmark_config.hpp"
#include "src/models/compression_result.hpp"
#include "src/schema/config_creator.hpp"
#include "src/utils/error_handler.hpp"

void printUsage(const char* programName) {
    std::cout << "Usage: " << programName << " [--log-errors] [--schema <schema_name>] <duckdb_file> <output_csv>\n";
    std::cout << "  --log-errors:      Log errors to stderr instead of throwing exceptions (optional)\n";
    std::cout << "  --schema <name>:   Filter to specific schema name (optional)\n";
    std::cout << "  duckdb_file:       Path to the DuckDB database file\n";
    std::cout << "  output_csv:        Path to the output CSV file\n";
}

int main(int argc, char* argv[]) {
    // Parse arguments
    std::vector<std::string> positional_args;
    bool log_errors = false;
    std::string schema_name = "";

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--log-errors") {
            log_errors = true;
        } else if (arg == "--schema") {
            if (i + 1 < argc) {
                schema_name = argv[++i];
            } else {
                std::cerr << "Error: --schema requires a value\n\n";
                printUsage(argv[0]);
                return 1;
            }
        } else {
            positional_args.push_back(arg);
        }
    }

    if (positional_args.size() != 2) {
        std::cerr << "Error: Invalid number of arguments\n\n";
        printUsage(argv[0]);
        return 1;
    }

    const std::string duckdb_path = positional_args[0];
    const std::string output_csv = positional_args[1];

    // Set error handling mode
    ErrorHandler::SetLogErrorsMode(log_errors);

    try {
        duckdb::DuckDB db(duckdb_path);
        duckdb::Connection con(db);

        con.Query("PRAGMA threads=1");
        con.Query("SELECT version()")->GetValue(0,0).Print();

        const BenchmarkConfigMetaData meta = {
            2,
            1,
            {
                AlgorithType::FSST,
                AlgorithType::FSST12,
                AlgorithType::OnPair16,
                AlgorithType::Dictionary,
                AlgorithType::LZ4
            }
        };
        const auto config = GetBenchmarkFromDatabase(con, meta, schema_name);

        const auto results = RunExperiment(con, config);
        SaveResultsAsCSV(results, output_csv);

        std::cout << "Benchmark completed successfully. Results saved to: " << output_csv << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}