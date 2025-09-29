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
        return decompressed_size + 32; // OnPair16 might write out of bounds for performance
    }

    void CompressAll(const StringCollector &data) override {
        on_pair16_ = OnPair16(data.Size(), data.TotalBytes());
        on_pair16_.compress_bytes(data.Data(), data.GetOffsets());
        compressed_size_ = on_pair16_.space_used();
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

    size_t CompressedSize() override {
        if (!compressed_ready_) throw std::logic_error("CompressedSize called before CompressAll/Benchmark");
        return compressed_size_;
    }

    void Free() override {
        // No dynamic resources to free in this implementation
    }

private:
    OnPair16 on_pair16_{};
    size_t   compressed_size_{0};
    bool     compressed_ready_{false};
};
