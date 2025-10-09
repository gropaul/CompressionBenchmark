#pragma once

#include <stdexcept>

#include "interface.hpp"
#include "onpair16.h"


class OnPair16Algorithm final : public ICompressionAlgorithm {
public:

    OnPair16Algorithm() = default;

    AlgorithType GetAlgorithmType() const override {
        return AlgorithType::OnPair16;
    }

    void Initialize(const ExperimentInput &input) override {}

    idx_t GetDecompressionBufferSize(const idx_t decompressed_size) override {
        return decompressed_size + 64; // OnPair16 might write out of bounds for performance
    }

    void CompressAll(const StringCollector &data) override {
        on_pair16_ = OnPair16(data.Size(), data.TotalBytes());
        on_pair16_.compress_bytes(data.Data(), data.GetOffsets());
        compressed_ready_ = true;
    }

    void DecompressAll(uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) throw std::logic_error("DecompressAll called before CompressAll/Benchmark");
        on_pair16_.decompress_all(out);
    }

    idx_t DecompressOne(size_t index, uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) throw std::logic_error("DecompressOne called before CompressAll/Benchmark");
        return on_pair16_.decompress_string(index, out);
    }

    CompressedSizeInfo CompressedSize() override {
        const std::vector<size_t> compressed_string_lengths = on_pair16_.compressed_string_lengths();
        const size_t data_lengths_size = BitPackingUtils::GetCompressedSize(compressed_string_lengths);

        return CompressedSizeInfo::OnPair(
            on_pair16_.space_used_dict_strings(),
            on_pair16_.space_used_dict_lengths(),
            on_pair16_.space_used_data_codes(),
            data_lengths_size
        );
    }

    void Free() override {
        // No dynamic resources to free in this implementation
    }

private:
    OnPair16 on_pair16_{};
    bool     compressed_ready_{false};
};
