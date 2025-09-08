


#include "src/benchmarker.hpp"
#include "src/models/benchmark_config.hpp"
#include "src/models/compression_result.hpp"
#include "src/schema/config_creator.hpp"

int main() {
    duckdb::DuckDB db("/Users/paul/workspace/SqlPile/data/sql_storm/imdb/imdb.duckdb");
    duckdb::Connection con(db);

    con.Query("PRAGMA threads=1");
    con.Query("SELECT version()")->Print();

    const BenchmarkConfigMetaData meta = {
        5,
        {
            CompressionAlgorithm::FSST,
            CompressionAlgorithm::OnPair16
        }
    };
    const auto config = GetBenchmarkFromDatabase(con, meta);


    RunExperiment(con, config);
}
