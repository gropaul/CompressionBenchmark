#include "duckdb.hpp"
#include "impl_fsst.hpp"
#include "impl_fsst12.hpp"
#include "impl_onpair16.hpp"
#include "impl_dictionary.hpp"
#include "../models/compression_result.hpp"
#include "../models/string_collection.hpp"

// returns a vector of size n with random numbers in the range [0, max)
inline std::vector<idx_t> GenerateRandomIndices(size_t n, size_t max) {
    std::vector<idx_t> indices(n);
    for (idx_t i = 0; i < n; i++) {
        indices[i] = rand() % max;
    }
    return indices;
}


inline AlgorithmResult Compress(const AlgorithType algorithm, const StringCollector &collector,
                                const size_t n_times) {
    std::vector<AlgorithmResult> results(n_times + 1);

    OnPair16Algorithm on_pair16;
    FsstAlgorithm fsst;
    Fsst12Algorithm fsst12;
    DictionaryAlgorithm dictionary;

    const auto random_row_indices = GenerateRandomIndices(N_RANDOM_ROW_ACCESSES, collector.Size());
    const auto random_vector_indices = GenerateRandomIndices(N_RANDOM_VECTOR_ACCESSES, collector.Size() / VECTOR_SIZE);
    const ExperimentInput input{const_cast<StringCollector &>(collector), random_row_indices, random_vector_indices};

    for (size_t run_idx = 0; run_idx < n_times + 1; run_idx += 1) {
        switch (algorithm) {
            case AlgorithType::FSST:
                results[run_idx] = fsst.Benchmark(input);
                break;
            case AlgorithType::FSST12:
                results[run_idx] = fsst12.Benchmark(input);
                break;
            case AlgorithType::OnPair16:
                results[run_idx] = on_pair16.Benchmark(input);
                break;
            case AlgorithType::Dictionary:
                results[run_idx] = dictionary.Benchmark(input);
                break;
            default:
                throw duckdb::Exception(duckdb::ExceptionType::INTERNAL, "Not know!");
        }
    }

    AlgorithmResult mean_result = MeanTimes(
        std::vector<AlgorithmResult>(results.begin() + 1, results.end())
    );
    return mean_result;
}
