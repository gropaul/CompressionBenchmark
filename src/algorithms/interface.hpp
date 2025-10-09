#pragma once
#include "../models/benchmark_config.hpp"


// An abstract interface for compression algorithms
class ICompressionAlgorithm {
public:
    virtual ~ICompressionAlgorithm() = default;

    // allocate buffers here if needed
    virtual void Initialize(const ExperimentInput &input) = 0;
    virtual AlgorithType GetAlgorithmType() const = 0;

    // Run a full benchmark (compress + decompress + timing)
    AlgorithmResult Benchmark(const ExperimentInput &input) {
        this->Initialize(input);
        using clock = std::chrono::high_resolution_clock;

        // *** Compression ***

        const auto t0 = clock::now();
        this->CompressAll(input.collector);
        const auto t1 = clock::now();
        const auto compression_info = this->CompressedSize();

        // *** Decompression (ALL) ***

        const idx_t decompression_buffer_size = this->GetDecompressionBufferSize(input.collector.TotalBytes());
        auto *decompression_buffer = static_cast<uint8_t *>(malloc(decompression_buffer_size));

        const auto t2 = clock::now();
        this->DecompressAll(decompression_buffer, decompression_buffer_size);
        const auto t3 = clock::now();

        const auto full_decompression_hash = duckdb::Hash(decompression_buffer, decompression_buffer_size);

        // check whether the decompressed data matches the original data
        const int full_cmp_result = std::memcmp(decompression_buffer, input.collector.Data(), input.collector.TotalBytes());
        if (full_cmp_result != 0) {
            throw std::runtime_error("Full decompression data does not match original data");
        }
        free(decompression_buffer);

        // *** Decompression (RANDOM ROWS) ***

        idx_t bytes_to_write = 0;
        for (const auto row_idx: input.random_row_indices) {
            const auto row_size = input.collector.GetLength(row_idx);
            bytes_to_write += row_size;
        }
        const idx_t random_decompression_buffer_size = this->GetDecompressionBufferSize(bytes_to_write);
        auto *random_decompression_buffer = static_cast<uint8_t *>(malloc(random_decompression_buffer_size));

        const auto t4 = clock::now();
        idx_t total_bytes_written = 0;
        for (const auto row_idx: input.random_row_indices) {
            const idx_t bytes_written = this->DecompressOne(row_idx, random_decompression_buffer + total_bytes_written, random_decompression_buffer_size);
            total_bytes_written += bytes_written;
        }
        const auto t5 = clock::now();

        // check whether the decompressed data matches the original data
        const uint8_t* current_buffer_position = random_decompression_buffer;
        std::vector<const unsigned char *> original_pointers = input.collector.GetPointers();
        for (const auto row_idx: input.random_row_indices) {
            const auto original_row_size = input.collector.GetLength(row_idx);
            const auto original_row_ptr = original_pointers[row_idx];
            const auto decompressed_row_ptr = current_buffer_position;

            if (std::memcmp(original_row_ptr, decompressed_row_ptr, original_row_size) != 0) {
                // print both the strings to help debugging
                printf("Original:     ");
                for (idx_t i = 0; i < original_row_size; i++) {
                    printf("%c ", original_row_ptr[i]);
                }
                printf("\nDecompressed: ");
                for (idx_t i = 0; i < original_row_size; i++) {
                    printf("%c ", decompressed_row_ptr[i]);
                }
                printf("\n");
                throw std::runtime_error("Random row decompression data does not match original data at row " + std::to_string(row_idx));
            }

            current_buffer_position += original_row_size;
        }

        const auto random_decompression_hash = duckdb::Hash(random_decompression_buffer, random_decompression_buffer_size);
        free(random_decompression_buffer);

        // *** Decompression (RANDOM VECTORS) ***

        bytes_to_write = 0;
        for (const auto vector_idx: input.random_vector_indices) {
            const idx_t start_row = vector_idx * VECTOR_SIZE;
            // if the end is larger than the number of rows, continue!
            if (start_row + VECTOR_SIZE >= input.collector.Size()) {
                continue;
            }

            const idx_t end_row = start_row + VECTOR_SIZE;
            for (idx_t row_idx = start_row; row_idx < end_row; row_idx++) {
                const auto row_size = input.collector.GetLength(row_idx);
                bytes_to_write += row_size;
            }
        }

        const idx_t vector_decompression_buffer_size = this->GetDecompressionBufferSize(bytes_to_write);

        const auto t6 = clock::now();

        auto *vector_decompression_buffer = static_cast<uint8_t *>(malloc(vector_decompression_buffer_size));


        for (const auto vector_idx: input.random_vector_indices) {
            const idx_t start_row = vector_idx * VECTOR_SIZE;
            // if the end is larger than the number of rows, continue!
            if (start_row + VECTOR_SIZE >= input.collector.Size()) {
                continue;
            }
            const idx_t end_row = start_row + VECTOR_SIZE;
            idx_t vector_sizes = 0;
            for (idx_t row_idx = start_row; row_idx < end_row; row_idx++) {
                const auto row_size = input.collector.GetLength(row_idx);
                vector_sizes += row_size;
            }

            for (idx_t row_idx = start_row; row_idx < end_row; row_idx++) {
                this->DecompressOne(row_idx, vector_decompression_buffer, vector_decompression_buffer_size);
            }
        }
        const auto t7 = clock::now();
        const auto vector_decompression_hash = duckdb::Hash(vector_decompression_buffer, vector_decompression_buffer_size);
        free(vector_decompression_buffer);


        // *** Cleanup ***
        this->Free();
        const auto compression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        const auto full_decompression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count();
        const auto random_decompression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t5 - t4).count();
        const auto vector_decompression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t7 - t6).count();
        //
        // printf("Algorithm %s: Compressed size: %llu bytes, compression time: %.3f ms, full decompression time: %.3f ms, vector decompression time: %.3f ms, random decompression time: %.3f ms\n",
        //        ToString(this->GetAlgorithmType()).c_str(),
        //        static_cast<unsigned long long>(compressed_size),
        //        compression_duration_ns / 1e6,
        //        full_decompression_duration_ns / 1e6,
        //        vector_decompression_duration_ns / 1e6,
        //        random_decompression_duration_ns / 1e6
        // );
        //
        // printf("  Hashes: full: %llu, vector: %llu, random: %llu\n",
        //        static_cast<unsigned long long>(full_decompression_hash),
        //        static_cast<unsigned long long>(vector_decompression_hash),
        //        static_cast<unsigned long long>(random_decompression_hash)
        // );

        return {
            this->GetAlgorithmType(),
            compression_info,
            compression_duration_ns / 1e6,
            full_decompression_duration_ns / 1e6,
            vector_decompression_duration_ns / 1e6,
            random_decompression_duration_ns / 1e6,
            full_decompression_hash,
            vector_decompression_hash,
            random_decompression_hash
        };


    }

    // Prepare an internal state by compressing all strings in the collector
    virtual void CompressAll(const StringCollector &data) = 0;

    virtual idx_t GetDecompressionBufferSize(idx_t decompressed_size) = 0;
    virtual void DecompressAll(uint8_t *out, size_t out_capacity) = 0;

    // returns the number of bytes written to out
    virtual idx_t DecompressOne(size_t index, uint8_t *out, size_t out_capacity) = 0;

    virtual CompressedSizeInfo CompressedSize() = 0;

    virtual void Free() = 0;
};