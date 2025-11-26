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

        ErrorHandler::HandleLogicError("OnPairMiniAlgorithm: Unsupported BITS_PER_TOKEN");
        return AlgorithType::OnPairMini10; // fallback for logging mode
    }

    void Initialize(const ExperimentInput &input) override {}

    idx_t GetDecompressionBufferSize(const idx_t decompressed_size) override {
        return decompressed_size + 32; // OnPairMini might write out of bounds for performance
    }

    void CompressAll(const StringCollector &data) override {
        on_pair_mini_ = OnPairMini<BITS_PER_TOKEN>(data.Size(), data.TotalBytes());
        on_pair_mini_.compress_bytes(data.Data(), data.GetOffsets());
        compressed_ready_ = true;
    }

    void DecompressAll(uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) ErrorHandler::HandleLogicError("DecompressAll called before CompressAll/Benchmark");
        on_pair_mini_.decompress_all(out);
    }

    idx_t DecompressOne(size_t index, uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) ErrorHandler::HandleLogicError("DecompressOne called before CompressAll/Benchmark");
        return on_pair_mini_.decompress_string(index, out);
    }

    CompressedSizeInfo CompressedSize() override {
        const std::vector<size_t> compressed_string_lengths = on_pair_mini_.compressed_string_lengths();
        const size_t data_lengths_size = BitPackingUtils::GetCompressedSize(compressed_string_lengths);

        return CompressedSizeInfo::OnPair(
            on_pair_mini_.space_used_dict_strings(),
            on_pair_mini_.space_used_dict_lengths(),
            on_pair_mini_.space_used_data_codes(),
            data_lengths_size
        );
    }

    void Free() override {
        // No dynamic resources to free in this implementation
    }

private:
    OnPairMini<BITS_PER_TOKEN> on_pair_mini_{};
    bool         compressed_ready_{false};
};