#pragma once

#include <vector>
#include <cstdint>
#include <cstring>
#include <cmath>
#include "interface.hpp"
#include "../../external/robin_hood/robin_hood.h"

class DictionaryAlgorithm final : public ICompressionAlgorithm {
public:
    DictionaryAlgorithm() = default;

    [[nodiscard]] AlgorithType GetAlgorithmType() const override {
        return AlgorithType::Dictionary;
    }

    void Initialize(const ExperimentInput &input) override {

    }

    idx_t GetDecompressionBufferSize(const idx_t decompressed_size) override {
        return decompressed_size + 32; // Small offset for safety
    }

    void CompressAll(const StringCollector &data) override {
        // Clear previous state
        dictionary.clear();
        dictionary_order.clear();

        const auto pointers = data.GetPointers();
        const auto lengths = data.GetLengths();

        // size the dictionary and the compressed indices to avoid reallocations
        dictionary.reserve(data.Size() / 10 + 1); // assume 10% unique strings
        dictionary_order.reserve(data.Size() / 10 + 1);
        compressed_indices.resize(data.Size());

        // Build dictionary and create index array
        for (size_t i = 0; i < data.Size(); i++) {

            const uint8_t* ptr = pointers[i];
            const size_t len = lengths[i];

            // Create a string view for lookup
            std::string_view str_view(reinterpret_cast<const char*>(ptr), len);

            // Check if string is already in dictionary
            auto it = dictionary.find(str_view);
            if (it == dictionary.end()) {
                // New unique string - add to dictionary
                auto dict_idx = static_cast<uint32_t>(dictionary_order.size());

                // Store in dictionary with index
                dictionary[str_view] = dict_idx;

                // Store actual string data in dictionary_order
                dictionary_order.emplace_back(ptr, len);

                // Store index for this row
                compressed_indices[i] = dict_idx;
            } else {
                // String already exists - store its index
                compressed_indices[i] = it->second;
            }
        }

        compressed_ready_ = true;
    }

    inline void DecompressAll(uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) ErrorHandler::HandleLogicError("DecompressAll called before CompressAll/Benchmark");

        uint8_t *write_ptr = out;

        for (size_t i = 0; i < compressed_indices.size(); i++) {
            const uint32_t dict_idx = compressed_indices[i];
            const auto& [str_ptr, str_len] = dictionary_order[dict_idx];

            std::memcpy(write_ptr, str_ptr, str_len);
            write_ptr += str_len;
        }
    }

    inline idx_t DecompressOne(size_t index, uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) ErrorHandler::HandleLogicError("DecompressOne called before CompressAll/Benchmark");

        const uint32_t dict_idx = compressed_indices[index];
        const auto& [str_ptr, str_len] = dictionary_order[dict_idx];

        std::memcpy(out, str_ptr, str_len);
        return str_len;
    }

    CompressedSizeInfo CompressedSize() override {
        if (!compressed_ready_) ErrorHandler::HandleLogicError("CompressedSize called before CompressAll/Benchmark");

        // Calculate dictionary size (all unique strings)
        size_t dictionary_strings_size = 0;
        std::vector<size_t> dictionary_lengths;
        for (const auto& [ptr, len] : dictionary_order) {
            dictionary_strings_size += len;
            dictionary_lengths.push_back(len);
        }

        const size_t dictionary_lengths_size = BitPackingUtils::GetCompressedSize(dictionary_lengths);

        // the codes will be bitpacked, so we need to calculate the size in bits
        const auto n_symbols = dictionary_order.size();
        const size_t data_codes_size = BitPackingUtils::GetCompressedSize(n_symbols, compressed_indices.size());
        return CompressedSizeInfo::Dictionary(dictionary_strings_size, dictionary_lengths_size, data_codes_size);
    }

    void Free() override {
        dictionary.clear();
        dictionary_order.clear();
        compressed_indices.clear();
    }

private:
    bool compressed_ready_{false};

    // Map from string to dictionary index
    robin_hood::unordered_map<std::string_view, uint32_t> dictionary;

    // Dictionary in insertion order: stores (pointer, length) pairs
    std::vector<std::pair<const uint8_t*, size_t>> dictionary_order;

    // Compressed data: indices into the dictionary
    std::vector<uint32_t> compressed_indices;
};