#include "duckdb.hpp"
#include "impl_fsst.hpp"
#include "impl_fsst12.hpp"
#include "impl_onpair.hpp"
#include "impl_onpair16.hpp"
#include "impl_onpair_mini.hpp"
#include "impl_dictionary.hpp"
#include "impl_lz4.hpp"
#include "../models/compression_result.hpp"
#include "../models/string_collection.hpp"

// returns a vector of size n with random numbers in the range [0, max)
inline std::vector<idx_t> GenerateRandomIndices(size_t n, size_t max) {
    std::vector<idx_t> indices(n);
    for (idx_t i = 0; i < n; i++) {
        indices[i] = rand() % max;
    }
    // sort the indices to improve cache locality
    std::sort(indices.begin(), indices.end());
    return indices;
}


inline AlgorithmResult Compress(const AlgorithType algorithm, const ExperimentInput &input,
                                const size_t n_times) {
    std::vector<AlgorithmResult> results(n_times + 1);

    OnPairAlgorithm on_pair;
    OnPair16Algorithm on_pair16;
    OnPairMiniAlgorithm<10> on_pair_mini_10;
    OnPairMiniAlgorithm<12> on_pair_mini_12;
    OnPairMiniAlgorithm<14> on_pair_mini_14;
    FsstAlgorithm fsst;
    Fsst12Algorithm fsst12;
    DictionaryAlgorithm dictionary;
    LZ4Algorithm lz4;



    for (size_t run_idx = 0; run_idx < n_times + 1; run_idx += 1) {
        switch (algorithm) {
            case AlgorithType::FSST:
                results[run_idx] = fsst.Benchmark(input);
                break;
            case AlgorithType::FSST12:
                results[run_idx] = fsst12.Benchmark(input);
                break;
            case AlgorithType::OnPair:
                results[run_idx] = on_pair.Benchmark(input);
                break;
            case AlgorithType::OnPair16:
                results[run_idx] = on_pair16.Benchmark(input);
                break;
            case AlgorithType::OnPairMini10:
                results[run_idx] = on_pair_mini_10.Benchmark(input);
                break;
            case AlgorithType::OnPairMini12:
                results[run_idx] = on_pair_mini_12.Benchmark(input);
                break;
            case AlgorithType::OnPairMini14:
                results[run_idx] = on_pair_mini_14.Benchmark(input);
                break;
            case AlgorithType::Dictionary:
                results[run_idx] = dictionary.Benchmark(input);
                break;
            case AlgorithType::LZ4:
                results[run_idx] = lz4.Benchmark(input);
                break;
            default:
                throw duckdb::Exception(duckdb::ExceptionType::INTERNAL, "Not know!");
        }
    }

    AlgorithmResult mean_result = MeanTimes(
        std::vector(results.begin() + 1, results.end())
    );
    return mean_result;
}
