#pragma once

#include <stdexcept>

#include "interface.hpp"
#include "onpair_mini.h"


template<size_t BITS_PER_TOKEN>
class OnPairMiniAlgorithm final : public ICompressionAlgorithm {
public:

    OnPairMiniAlgorithm() = default;

    AlgorithType GetAlgorithmType() const override {
        if (BITS_PER_TOKEN == 10) return AlgorithType::OnPairMini10;
        if (BITS_PER_TOKEN == 12) return AlgorithType::OnPairMini12;
        if (BITS_PER_TOKEN == 14) return AlgorithType::OnPairMini14;

        throw std::logic_error("OnPairMiniAlgorithm: Unsupported BITS_PER_TOKEN");
    }

    void Initialize(const ExperimentInput &input) override {}

    idx_t GetDecompressionBufferSize(const idx_t decompressed_size) override {
        return decompressed_size + 32; // OnPairMini might write out of bounds for performance
    }

    void CompressAll(const StringCollector &data) override {
        on_pair_mini_ = OnPairMini<BITS_PER_TOKEN>(data.Size(), data.TotalBytes());
        on_pair_mini_.compress_bytes(data.Data(), data.GetOffsets());
        compressed_size_ = on_pair_mini_.space_used();
        compressed_ready_ = true;
    }

    void DecompressAll(uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) throw std::logic_error("DecompressAll called before CompressAll/Benchmark");
        on_pair_mini_.decompress_all(out);
    }

    idx_t DecompressOne(size_t index, uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) throw std::logic_error("DecompressOne called before CompressAll/Benchmark");
        return on_pair_mini_.decompress_string(index, out);
    }

    size_t CompressedSize() override {
        if (!compressed_ready_) throw std::logic_error("CompressedSize called before CompressAll/Benchmark");
        return compressed_size_;
    }

    void Free() override {
        // No dynamic resources to free in this implementation
    }

private:
    OnPairMini<BITS_PER_TOKEN> on_pair_mini_{};
    size_t       compressed_size_{0};
    bool         compressed_ready_{false};
};