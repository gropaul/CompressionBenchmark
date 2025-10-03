#pragma once

#include <stdexcept>
#include <lz4.h>

#include "interface.hpp"

class LZ4Algorithm final : public ICompressionAlgorithm {
public:
    explicit LZ4Algorithm(int block_size_limit = LZ4_MAX_INPUT_SIZE)
        : block_size_limit_(block_size_limit) {
    }

    [[nodiscard]] AlgorithType GetAlgorithmType() const override {
        return AlgorithType::LZ4;
    }

    void Initialize(const ExperimentInput &input) override {
        compressed_lengths.resize(input.collector.Size());
        compressed_pointers.resize(input.collector.Size());

        // LZ4_compressBound gives us the max size needed for compressed output
        compression_buffer_size = 0;
        for (size_t i = 0; i < input.collector.Size(); i++) {
            const size_t input_size = std::min(static_cast<size_t>(block_size_limit_),
                                               input.collector.GetLength(i));
            compression_buffer_size += LZ4_compressBound(input_size);
        }
        compression_buffer = static_cast<uint8_t *>(malloc(compression_buffer_size));
    }

    idx_t GetDecompressionBufferSize(const idx_t decompressed_size) override {
        return decompressed_size + 32; // Small offset for safety
    }

    void CompressAll(const StringCollector &data) override {
        uint8_t *write_ptr = compression_buffer;

        const auto pointers = data.GetPointers();

        for (size_t i = 0; i < data.Size(); i++) {
            const uint8_t *input_ptr = pointers[i];
            const size_t input_size = std::min(static_cast<size_t>(block_size_limit_),
                                               data.GetLength(i));
            const int max_output_size = LZ4_compressBound(input_size);

            const int compressed_size = LZ4_compress_default(
                reinterpret_cast<const char*>(input_ptr),
                reinterpret_cast<char*>(write_ptr),
                input_size,
                max_output_size
            );

            if (compressed_size <= 0) {
                throw std::runtime_error("LZ4 compression failed");
            }

            compressed_pointers[i] = write_ptr;
            compressed_lengths[i] = compressed_size;
            write_ptr += compressed_size;
        }

        compressed_ready_ = true;
    }

    inline void DecompressAll(uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) throw std::logic_error("DecompressAll called before CompressAll/Benchmark");
        uint8_t *write_ptr = out;

        for (size_t i = 0; i < compressed_lengths.size(); i++) {
            const int decompressed_size = LZ4_decompress_safe(
                reinterpret_cast<const char*>(compressed_pointers[i]),
                reinterpret_cast<char*>(write_ptr),
                compressed_lengths[i],
                out_capacity - (write_ptr - out)
            );

            if (decompressed_size < 0) {
                throw std::runtime_error("LZ4 decompression failed");
            }

            write_ptr += decompressed_size;
        }
    }

    inline idx_t DecompressOne(size_t index, uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) throw std::logic_error("DecompressOne called before CompressAll/Benchmark");

        const int decompressed_size = LZ4_decompress_safe(
            reinterpret_cast<const char*>(compressed_pointers[index]),
            reinterpret_cast<char*>(out),
            compressed_lengths[index],
            out_capacity
        );

        if (decompressed_size < 0) {
            throw std::runtime_error("LZ4 decompression failed");
        }

        return decompressed_size;
    }

    size_t CompressedSize() override {
        if (!compressed_ready_) throw std::logic_error("CompressedSize called before CompressAll/Benchmark");
        size_t total_compressed_size = 0;
        for (const size_t length : compressed_lengths) {
            total_compressed_size += length;
        }
        return total_compressed_size;
    }

    void Free() override {
        free(compression_buffer);
    }

private:
    bool compressed_ready_{false};
    int block_size_limit_;

    // buffer for the compressed data
    idx_t compression_buffer_size;
    uint8_t *compression_buffer;
    std::vector<size_t> compressed_lengths;
    std::vector<uint8_t *> compressed_pointers;
};