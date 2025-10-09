


#include "src/benchmarker.hpp"
#include "src/models/benchmark_config.hpp"
#include "src/models/compression_result.hpp"
#include "src/schema/config_creator.hpp"

int main() {
    // duckdb::DuckDB db("/Users/paul/workspace/SqlPile/data/kaggle/kaggle_data.duckdb");
    duckdb::DuckDB db("/Users/paul/workspace/SqlPile/data/sql_storm/imdb/imdb.duckdb");
    // duckdb::DuckDB db("/Users/paul/tpch-10.duckdb");
    duckdb::Connection con(db);

    con.Query("PRAGMA threads=1");
    con.Query("SELECT version()")->GetValue(0,0).Print();

    const BenchmarkConfigMetaData meta = {
        3,
        1,
        {
            // AlgorithType::FSST,
            // AlgorithType::FSST12,
            AlgorithType::OnPair,
            // AlgorithType::OnPair16,
            // AlgorithType::OnPairMini10,
            // AlgorithType::OnPairMini12,
            // AlgorithType::OnPairMini14,
            // AlgorithType::Dictionary,
            // AlgorithType::LZ4
        }
    };
    const auto config = GetBenchmarkFromDatabase(con, meta);


    const auto results = RunExperiment(con, config);
    SaveResultsAsCSV(results, "/Users/paul/workspace/SqlPile/external/CompressionBenchmark/results.csv");
}
