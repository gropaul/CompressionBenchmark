# pragma once
#include <duckdb.h>
#include <cmath>



class BitPackingUtils {
public:

    static uint8_t GetBitsPerValue(const idx_t range) {
        if (range == 0) return 1;
        return static_cast<uint8_t>(std::ceil(std::log2(range + 1)));
    }

    static idx_t GetCompressedSize(const idx_t range, const idx_t n_values) {
        const auto bits_per_length = GetBitsPerValue(range);
        const size_t length_storage_size = (bits_per_length * n_values + 7) / 8; // in bytes
        return length_storage_size;
    }


    template <typename T>
    static idx_t GetCompressedSize(const std::vector<T>& values) {
        if (values.empty()) return 0;
        const auto [min_it, max_it] = std::minmax_element(values.begin(), values.end());
        const auto range = static_cast<idx_t>(*max_it - *min_it);
        return GetCompressedSize(range, values.size());
    }

};
