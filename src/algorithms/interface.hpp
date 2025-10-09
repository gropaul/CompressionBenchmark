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

        const std::vector<size_t> string_lengths = input.collector.GetLengths();
        bool has_error = false;
        std::string error_message = "";

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

        // *** Decompression Check (ALL)  ***

        const int full_cmp_result = std::memcmp(decompression_buffer, input.collector.Data(),
                                                input.collector.TotalBytes());
        if (full_cmp_result != 0) {
            // go through both buffers and print the first difference
            for (idx_t i = 0; i < input.collector.TotalBytes(); i++) {
                if (decompression_buffer[i] != input.collector.Data()[i]) {
                    error_message += "Full decompression data does not match original data: first difference at byte " +
                            std::to_string(i) +
                            " (original: " + std::to_string(input.collector.Data()[i]) +
                            ", decompressed: " + std::to_string(decompression_buffer[i]) + ")\n";
                    break;
                }
            }

            has_error = true;
        }
        free(decompression_buffer);

        // *** Decompression (RANDOM ROWS) ***

        idx_t bytes_to_write = 0;
        for (const auto row_idx: input.random_row_indices) {
            const auto row_size = string_lengths[row_idx];
            bytes_to_write += row_size;
        }

        const idx_t random_decompression_buffer_size = this->GetDecompressionBufferSize(bytes_to_write);
        auto *random_decompression_buffer = static_cast<uint8_t *>(malloc(random_decompression_buffer_size));
        uint8_t *random_decompression_buffer_write_ptr = random_decompression_buffer;
        const auto t4 = clock::now();
        for (const auto row_idx: input.random_row_indices) {
            const idx_t remaining_capacity = random_decompression_buffer_size - (
                                                 random_decompression_buffer_write_ptr - random_decompression_buffer);
            const idx_t bytes_written = this->DecompressOne(row_idx, random_decompression_buffer_write_ptr,
                                                            remaining_capacity);

            if (remaining_capacity > random_decompression_buffer_size) {
                throw std::runtime_error("Decompression wrote out of bounds");
            }
            random_decompression_buffer_write_ptr += bytes_written;
        }
        const auto t5 = clock::now();

        // *** Decompression Check (RANDOM ROWS) ***

        // make sure we didn't write out of bounds
        if (random_decompression_buffer_write_ptr - random_decompression_buffer != bytes_to_write) {
            error_message += "Random row decompression wrote " +
                    std::to_string(random_decompression_buffer_write_ptr - random_decompression_buffer) +
                    " bytes, but expected " + std::to_string(bytes_to_write) + " bytes\n";
            has_error = true;
            throw std::runtime_error("Decompression wrote unexpected number of bytes");
        }

        const uint8_t *random_decompression_buffer_check_ptr = random_decompression_buffer;
        std::vector<const unsigned char *> original_pointers = input.collector.GetPointers();
        for (const auto row_idx: input.random_row_indices) {
            const auto original_row_size = input.collector.GetLength(row_idx);
            const auto original_row_ptr = original_pointers[row_idx];
            const auto decompressed_row_ptr = random_decompression_buffer_check_ptr;

            if (std::memcmp(original_row_ptr, decompressed_row_ptr, original_row_size) != 0) {
                error_message += "Random row decompression data does not match original data: first difference at row "
                        + std::to_string(row_idx) + "\n";
                has_error = true;
            }

            random_decompression_buffer_check_ptr += original_row_size;
        }

        const auto random_decompression_hash = duckdb::Hash(random_decompression_buffer,
                                                            random_decompression_buffer_size);
        free(random_decompression_buffer);

        // *** Decompression (RANDOM VECTORS) ***

        bytes_to_write = 0;
        for (const auto vector_idx: input.random_vector_indices) {
            const idx_t start_row = vector_idx * VECTOR_SIZE;
            const idx_t end_row = start_row + VECTOR_SIZE;

            if (start_row + VECTOR_SIZE > input.collector.Size()) { continue; }

            for (idx_t row_idx = start_row; row_idx < end_row; row_idx++) {
                const auto row_size = string_lengths[row_idx];
                bytes_to_write += row_size;
            }
        }

        const idx_t vector_decompression_buffer_size = this->GetDecompressionBufferSize(bytes_to_write);
        auto *vector_decompression_buffer = static_cast<uint8_t *>(malloc(vector_decompression_buffer_size));
        uint8_t *vector_decompression_buffer_write_ptr = vector_decompression_buffer;

        const auto t6 = clock::now();

        for (const auto vector_idx: input.random_vector_indices) {
            const idx_t start_row = vector_idx * VECTOR_SIZE;
            const idx_t end_row = start_row + VECTOR_SIZE;

            if (start_row + VECTOR_SIZE > input.collector.Size()) { continue; }


            for (idx_t row_idx = start_row; row_idx < end_row; row_idx++) {
                const idx_t remaining_capacity = vector_decompression_buffer_size - (
                                                     vector_decompression_buffer_write_ptr -
                                                     vector_decompression_buffer);
                const size_t bytes_written = this->DecompressOne(row_idx, vector_decompression_buffer_write_ptr,
                                                                 remaining_capacity);
                vector_decompression_buffer_write_ptr += bytes_written;
            }
        }
        const auto t7 = clock::now();

        const auto vector_decompression_hash = duckdb::Hash(vector_decompression_buffer,
                                                            vector_decompression_buffer_size);
        free(vector_decompression_buffer);

        // todo: check vector decompression


        // *** Cleanup ***
        this->Free();
        const auto compression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        const auto full_decompression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).
                count();
        const auto random_decompression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t5 - t4).
                count();
        const auto vector_decompression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t7 - t6).
                count();

        return {
            this->GetAlgorithmType(),
            compression_info,
            has_error,
            error_message,
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
