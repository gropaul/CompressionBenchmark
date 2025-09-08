#pragma once
#include "onpair16.h"
#include "../models/string_collection.hpp"
#include "../models/compression_result.hpp"

inline AlgorithmResult CompressOnPair16(const StringCollector &collector) {
    using clock = std::chrono::high_resolution_clock;
    auto t0 = clock::now();
    OnPair16 on_pair16(collector.Size(), collector.TotalBytes());

    on_pair16.compress_bytes(collector.Data(), collector.GetOffsets());

    const auto compressed_size = on_pair16.space_used();
    const auto t1 = clock::now();

    const size_t compression_buffer_size = collector.TotalBytes() + 32;
    unsigned char *compression_buffer = reinterpret_cast<uint8_t*>(malloc(compression_buffer_size));
    // on_pair16.decompress_all(result_buffer);
    //
    // todo: this is quiet interesting that this is much faster to decode everything

    for (size_t index = 0; index < collector.Size(); index ++) {
        on_pair16.decompress_string(index, compression_buffer);
    }

    auto t2 = clock::now();

    free(compression_buffer);

    auto compression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    auto decompression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();


    return {
        CompressionAlgorithm::OnPair16,
        compressed_size,
        compression_duration_ns / 1e6,
        decompression_duration_ns / 1e6
    };
}
