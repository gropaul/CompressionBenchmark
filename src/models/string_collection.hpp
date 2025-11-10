#pragma once


#include <duckdb.h>
#include <iostream>
#include <vector>

// If size_t isn't defined elsewhere, uncomment this:
// using size_t = std::size_t;

class StringCollector {
    std::vector<size_t> offsets;
    uint8_t *data = nullptr;

public:
    explicit StringCollector(std::size_t initial_byte_capacity = 0,
                             std::size_t expected_strings = 0)
        : data(nullptr), size_bytes_(0), capacity_bytes_(0) {
        if (initial_byte_capacity) ReserveBytes(initial_byte_capacity);
        if (expected_strings) offsets.reserve(expected_strings);
    }

    ~StringCollector() {
        delete[] data;
    }

    StringCollector(const StringCollector &) = delete;
    StringCollector &operator=(const StringCollector &) = delete;


    // Append a string; returns its index.
    size_t AddString(std::string_view s) {
        const std::size_t need = s.size();
        const auto start = static_cast<size_t>(size_bytes_);

        EnsureCapacity(size_bytes_ + need);

        // copy payload
        if (!s.empty()) std::memcpy(data + size_bytes_, s.data(), s.size());
        size_bytes_ += s.size();

        offsets.push_back(start);
        return offsets.size() - 1;
    }

    // Append a string; returns its index.
    size_t AddStringDDB(duckdb::string_t s) {
        const std::size_t size = s.GetSize();
        const auto start = static_cast<size_t>(size_bytes_);

        EnsureCapacity(size_bytes_ + size);

        // copy payload
        if (size != 0) {
            std::memcpy(data + size_bytes_, s.GetData(), size);
        }
        size_bytes_ += size;

        offsets.push_back(start);
        return offsets.size() - 1;
    }


    // Number of stored strings
    std::size_t Size() const noexcept { return offsets.size(); }

    // Raw buffer info (useful if you need to serialize)
    const uint8_t *Data() const noexcept { return data; }
    std::size_t TotalBytes() const noexcept { return size_bytes_; }
    std::size_t TotalSizeLengths() const noexcept { return offsets.size() * sizeof(uint32_t); }
    // Includes the size of the length array and the data array, don't confuse it with TotalBytes
    std::size_t TotalSizeRequired() const noexcept { return TotalBytes() + TotalSizeLengths(); }
    std::size_t ByeCapacity() const noexcept { return capacity_bytes_; }

    // Will include n+1 offsets where offset[i] + start is the pointer of i
    // and offset[i + 1] - offset[i] is its length
    std::vector<size_t> GetOffsets() const {

        std::vector<size_t> starts(offsets);
        starts.push_back(TotalBytes());
        return starts;
    }

    // Will include n+1 addresses where ptr[i] is the pointer of i and ptr[i + 1] - ptr[i] is its length
    std::vector<const unsigned char *> GetPointers() const {
        const auto offsets = GetOffsets();
        std::vector<const unsigned char *> ptrs(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            ptrs[i] = data + offsets[i];
        }
        return ptrs;
    }

    size_t GetLength(size_t idx) const {
        const size_t start = offsets[idx];
        size_t end = size_bytes_;
        if (idx < offsets.size() - 1) {
            end = offsets[idx + 1];
        }
        const size_t length = end - start;
        return length;
    }

    // Get the string sizes
    std::vector<size_t> GetLengths() const {
        const auto n = Size();

        std::vector<size_t> lengths(n);
        for (size_t idx = 0; idx < n; idx++) {
            const auto length = GetLength(idx);
            lengths[idx] = length;
        }

        return lengths;
    }

    // Pre-reserve to reduce reallocations
    void ReserveBytes(std::size_t cap) { ReserveBytesImpl(cap); }
    void ReserveStrings(std::size_t n) { offsets.reserve(n); }

    // Clear contents but keep capacity
    void Clear() {
        offsets.clear();
        size_bytes_ = 0;
        if (data && capacity_bytes_ > 0) data[0] = 0;
    }

    std::string Get(size_t i) const {
        assert(i < offsets.size());

        const size_t start = offsets[i];
        const size_t length = GetLength(i);

        const char *p = reinterpret_cast<const char *>(data + start);

        return {p, length};
    }


    void Print(size_t n = 0) const {
        if (n == 0) {
            n = Size();
        }
        std::cout << "StringCollector with " << Size() << " strings:\n";
        for (size_t i = 0; i < n; ++i) {
            std::cout << " [" << i << "] \"" << Get(i) << "\"\n";
        }
    }

private:
    std::size_t size_bytes_ = 0; // used bytes in data
    std::size_t capacity_bytes_ = 0; // allocated bytes for data

    void EnsureCapacity(std::size_t min_cap) {
        if (min_cap <= capacity_bytes_) return;
        // grow strategy: 1.5x + slack
        std::size_t new_cap = capacity_bytes_ ? capacity_bytes_ + capacity_bytes_ / 2 : 64;
        if (new_cap < min_cap) new_cap = min_cap;
        ReserveBytesImpl(new_cap);
    }

    void ReserveBytesImpl(std::size_t new_cap) {
        if (new_cap <= capacity_bytes_) return;
        auto *new_data = new uint8_t[new_cap];
        if (data && size_bytes_) std::memcpy(new_data, data, size_bytes_);
        delete[] data;
        data = new_data;
        capacity_bytes_ = new_cap;
        if (size_bytes_ == 0) data[0] = 0; // keep a valid c-string at start
    }
};
