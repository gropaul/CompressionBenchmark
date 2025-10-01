


#include "src/benchmarker.hpp"
#include "src/models/benchmark_config.hpp"
#include "src/models/compression_result.hpp"
#include "src/schema/config_creator.hpp"

int main() {
    duckdb::DuckDB db("/Users/paul/workspace/SqlPile/data/kaggle/kaggle_data.duckdb");
    duckdb::Connection con(db);

    con.Query("PRAGMA threads=1");
    con.Query("SELECT version()")->GetValue(0,0).Print();

    const BenchmarkConfigMetaData meta = {
        5,
        {
            AlgorithType::FSST,
            AlgorithType::FSST12,
            AlgorithType::OnPair16
        }
    };
    const auto config = GetBenchmarkFromDatabase(con, meta);


    const auto results = RunExperiment(con, config);
    SaveResultsAsCSV(results, "/Users/paul/workspace/SqlPile/external/CompressionBenchmark/results.csv");
}
