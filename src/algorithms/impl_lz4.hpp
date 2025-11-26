#pragma once

#include <stdexcept>
#include <lz4.h>

#include "interface.hpp"
#include "../utils/bitpacking_utils.hpp"

#define BLOCK_VECTOR_SIZE STANDARD_VECTOR_SIZE

struct Block {
    uint32_t uncompressed_lengths[BLOCK_VECTOR_SIZE];
    size_t uncompressed_data_size;
    size_t compressed_data_size;
    uint8_t* compressed_data;

    [[nodiscard]] size_t GetCompressedSize() const {
        return GetCompressedSizeData() + GetCompressedSizeLengths();
    }

    [[nodiscard]] size_t GetCompressedSizeData() const {
        return compressed_data_size;
    }

    [[nodiscard]] size_t GetCompressedSizeLengths() const {
        const size_t max_string_length = *std::max_element(uncompressed_lengths, uncompressed_lengths + BLOCK_VECTOR_SIZE);
        return BitPackingUtils::GetCompressedSize(max_string_length, BLOCK_VECTOR_SIZE);
    }
};

struct StringOffset {
    size_t string_idx;
    size_t string_offset;
};

class LZ4Algorithm final : public ICompressionAlgorithm {
public:


    explicit LZ4Algorithm() = default;

    [[nodiscard]] AlgorithType GetAlgorithmType() const override {
        return AlgorithType::LZ4;
    }

    void Initialize(const ExperimentInput &input) override {
        const size_t n_blocks = (input.collector.Size() + BLOCK_VECTOR_SIZE - 1) / BLOCK_VECTOR_SIZE;
        blocks_.resize(n_blocks);

        // Allocate the compression buffer with 2x the size of the input data. Add factor 1.5 as we do blocking and not
        // compress the full data at once
        const int total_input_size = static_cast<int>(static_cast<double>(input.collector.TotalBytes()) * 1.5 + 256);
        compression_buffer_size = LZ4_compressBound(total_input_size);
        compression_buffer = static_cast<uint8_t *>(malloc(compression_buffer_size));

        cached_block_index = std::numeric_limits<idx_t>::max();
        decompression_cache_ = nullptr;
        decompression_cache_size_ = 0;
        last_decompressed_ = {0, 0};
    }

    idx_t GetDecompressionBufferSize(const idx_t decompressed_size) override {
        return decompressed_size + 32; // Small offset for safety
    }

    void CompressAll(const StringCollector &data) override {

        const auto pointers = data.GetPointers();

        uint8_t* write_ptr = compression_buffer;

        for (size_t block_idx = 0; block_idx < blocks_.size(); block_idx++) {

            auto &block = blocks_[block_idx];

            const size_t string_start_idx = block_idx * BLOCK_VECTOR_SIZE;
            const size_t string_end_idx = std::min(string_start_idx + BLOCK_VECTOR_SIZE, data.Size());

            const uint8_t* input_ptr = pointers[string_start_idx];

            size_t input_size = 0;
            for (size_t i = string_start_idx; i < string_end_idx; i++) {
                const size_t length = data.GetLength(i);
                input_size += length;
                block.uncompressed_lengths[i - string_start_idx] = static_cast<uint32_t>(length);
            }

            const int max_output_size = static_cast<int>(compression_buffer_size - (write_ptr - compression_buffer));

            const int compressed_size = LZ4_compress_default(
                reinterpret_cast<const char*>(input_ptr),
                reinterpret_cast<char*>(write_ptr),
                static_cast<int>(input_size),
                max_output_size
            );

            block.uncompressed_data_size = input_size;
            block.compressed_data = write_ptr;
            block.compressed_data_size = compressed_size;
            write_ptr += compressed_size;
        }

        compressed_ready_ = true;
    }

    inline void DecompressAll(uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) ErrorHandler::HandleLogicError("DecompressAll called before CompressAll/Benchmark");

        uint8_t* write_ptr = out;
        size_t remaining_capacity = out_capacity;

        for (const auto &block: blocks_) {
            const int decompressed_size = LZ4_decompress_safe(
                reinterpret_cast<const char*>(block.compressed_data),
                reinterpret_cast<char*>(write_ptr),
                static_cast<int>(block.compressed_data_size),
                static_cast<int>(remaining_capacity)
            );

            if (decompressed_size < 0 || static_cast<size_t>(decompressed_size) != block.uncompressed_data_size) {
                ErrorHandler::HandleRuntimeError("LZ4 decompression failed or output size mismatch");
            }

            write_ptr += decompressed_size;
            remaining_capacity -= decompressed_size;
        }
    }

    inline Block& DecompressAndCacheBlock(const idx_t block_idx) {
        if (block_idx == cached_block_index) {
            return blocks_[block_idx];
        }

        if (decompression_cache_ == nullptr) {
            decompression_cache_size_ = 0;
            for (const auto &block: blocks_) {
                decompression_cache_size_ = std::max(decompression_cache_size_, block.uncompressed_data_size);
            }
            decompression_cache_ = static_cast<uint8_t *>(malloc(decompression_cache_size_));
        }

        const auto &block = blocks_[block_idx];
        const int decompressed_size = LZ4_decompress_safe(
            reinterpret_cast<const char*>(block.compressed_data),
            reinterpret_cast<char*>(decompression_cache_),
            static_cast<int>(block.compressed_data_size),
            static_cast<int>(decompression_cache_size_)
        );

        if (decompressed_size < 0 || static_cast<size_t>(decompressed_size) != block.uncompressed_data_size) {
            ErrorHandler::HandleRuntimeError("LZ4 decompression failed or output size mismatch");
        }

        // reset the last compressed string offset
        last_decompressed_ = {0, 0};

        cached_block_index = block_idx;
        return blocks_[block_idx];
    }

    inline idx_t DecompressOne(const size_t index, uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) ErrorHandler::HandleLogicError("DecompressAll called before CompressAll/Benchmark");

        const size_t block_idx = index / BLOCK_VECTOR_SIZE;
        const Block &block = DecompressAndCacheBlock(block_idx);

        const size_t string_idx_in_block = index % BLOCK_VECTOR_SIZE;
        const size_t string_length = block.uncompressed_lengths[string_idx_in_block];

        // copy the string to the output buffer
        if (string_length > out_capacity) {
            ErrorHandler::HandleRuntimeError("Output buffer too small for decompressed string");
        }

        if (string_idx_in_block < last_decompressed_.string_idx) {
            last_decompressed_ = {0, 0};
        }

        size_t string_offset = last_decompressed_.string_offset;
        for (size_t i = last_decompressed_.string_idx; i < string_idx_in_block; i++) {
            string_offset += block.uncompressed_lengths[i];
        }

        last_decompressed_ = {string_idx_in_block, string_offset};

        std::memcpy(out, decompression_cache_ + string_offset, string_length);

        return string_length;
    }

    CompressedSizeInfo CompressedSize() override {
        size_t compressed_size_data = 0;
        size_t compressed_size_lengths = 0;
        for (const auto &block: blocks_) {
            compressed_size_data += block.GetCompressedSizeData();
            compressed_size_lengths += block.GetCompressedSizeLengths();
        }
        return CompressedSizeInfo::LZ4(compressed_size_data, compressed_size_lengths);
    }

    void Free() override {
        free(compression_buffer);
        compression_buffer = nullptr;
        compression_buffer_size = 0;

        free(decompression_cache_);
        decompression_cache_ = nullptr;
        decompression_cache_size_ = 0;
    }

private:
    bool compressed_ready_{false};
    std::vector<Block> blocks_;

    // buffer for the compressed data
    idx_t compression_buffer_size;
    uint8_t *compression_buffer;

    // for caching decompressed blocks during random access decompression
    idx_t cached_block_index;
    uint8_t *decompression_cache_;
    size_t decompression_cache_size_;

    StringOffset last_decompressed_;
};
