#pragma once

#include <vector>
#include <string>
#include <iostream>
#include <iomanip>


enum class CompressionAlgorithm {
    FSST,
    FSST12,
    OnPair16,
};

inline std::string toString(CompressionAlgorithm algo) {
    switch (algo) {
        case CompressionAlgorithm::FSST:   return "FSST";
        case CompressionAlgorithm::FSST12: return "FSST12";
        case CompressionAlgorithm::OnPair16: return "OnPair16";
    }
    return "Unknown";
}

struct AlgorithmResult {
    CompressionAlgorithm algorithm;
    uint64_t compressed_size;
    double compression_time_ms;
    double decompression_time_ms;
};

class CompressionResult {
public:
    explicit CompressionResult(uint64_t uncompressed_size = 0)
        : uncompressed_size_(uncompressed_size) {}

    void setUncompressedSize(uint64_t size) { uncompressed_size_ = size; }
    uint64_t uncompressedSize() const { return uncompressed_size_; }

    void AddResult(const AlgorithmResult& res) {
        results_.push_back(res);
    }

    const std::vector<AlgorithmResult>& results() const {
        return results_;
    }

    void PrettyPrint(std::ostream& os = std::cout) const {
        os << "CompressionResult\n";
        os << "  Uncompressed size: " << uncompressed_size_ << " bytes\n";
        os << "  Algorithms:\n";
        for (auto& r : results_) {
            double factor = (r.compressed_size > 0)
                          ? static_cast<double>(uncompressed_size_) / r.compressed_size
                          : 0.0;


            os << "   - " << std::setw(7) << toString(r.algorithm)
               << ": " << r.compressed_size << " bytes"
               << " (" << std::fixed << std::setprecision(2) << factor << "× smaller)"
               << ", compression: " << std::setprecision(3) << r.compression_time_ms << " ms"
               << ", decompression: " << std::setprecision(3) << r.decompression_time_ms << " ms\n";
        }
    }

private:
    uint64_t uncompressed_size_;
    std::vector<AlgorithmResult> results_;
};
