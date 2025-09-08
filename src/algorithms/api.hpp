#include "duckdb.hpp"
#include "onpair.hpp"
#include "fsst.hpp"
#include "../models/compression_result.hpp"
#include "../models/string_collection.hpp"

inline AlgorithmResult Compress(const CompressionAlgorithm algorithm, const StringCollector &collector,
                                const size_t n_times) {
    std::vector<AlgorithmResult> results(n_times + 1);

    for (size_t run_idx = 0; run_idx < n_times + 1; run_idx += 1) {
        // printf("%llu\n",collector.GetOffsets()[0]);
        switch (algorithm) {
            case CompressionAlgorithm::FSST:
                results[run_idx] = CompressFSST(collector);
                break;
            case CompressionAlgorithm::OnPair16:
                results[run_idx] = CompressOnPair16(collector);
                break;
            default:
                throw duckdb::Exception(duckdb::ExceptionType::INTERNAL, "Not know!");
        }
    }

    AlgorithmResult mean_result{};
    mean_result.algorithm = algorithm;

    for (size_t i = 1; i < results.size(); i++) {
        mean_result.compressed_size += results[i].compressed_size;
        mean_result.compression_time_ms += results[i].compression_time_ms;
        mean_result.decompression_time_ms += results[i].decompression_time_ms;
    }

    const double denom = static_cast<double>(n_times);
    mean_result.compressed_size = static_cast<uint64_t>(mean_result.compressed_size / denom);
    mean_result.compression_time_ms /= denom;
    mean_result.decompression_time_ms /= denom;

    return mean_result;
}
