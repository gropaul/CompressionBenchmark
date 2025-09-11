#pragma once

#include <filesystem>
#include <fstream>
#include <vector>
#include <string>
#include <iostream>
#include <iomanip>
#include "../utils/csv_utils.hpp"


struct TableConfig;

enum class CompressionAlgorithm {
    FSST,
    FSST12,
    OnPair16,
};



inline std::string ToString(CompressionAlgorithm algo) {
    switch (algo) {
        case CompressionAlgorithm::FSST: return "FSST";
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

class ExperimentResult {
public:
    explicit ExperimentResult(
        const uint64_t uncompressed_size,
        const std::string &table_name,
        const std::string &column_name
    )
        : table_name_(table_name), column_name_(column_name), uncompressed_size_(uncompressed_size) {
    }

    void setUncompressedSize(uint64_t size) { uncompressed_size_ = size; }
    uint64_t uncompressedSize() const { return uncompressed_size_; }

    void AddResult(const AlgorithmResult &res) {
        results_.push_back(res);
    }

    const std::vector<AlgorithmResult> &results() const {
        return results_;
    }

    void PrettyPrint(std::ostream &os = std::cout) const {
        os << "CompressionResult\n";
        os << "  Uncompressed size: " << uncompressed_size_ << " bytes\n";
        os << "  Algorithms:\n";
        for (auto &r: results_) {
            double factor = (r.compressed_size > 0)
                                ? static_cast<double>(uncompressed_size_) / r.compressed_size
                                : 0.0;


            os << "   - " << std::setw(7) << ToString(r.algorithm)
                    << ": " << r.compressed_size << " bytes"
                    << " (" << std::fixed << std::setprecision(2) << factor << "× smaller)"
                    << ", compression: " << std::setprecision(3) << r.compression_time_ms << " ms"
                    << ", decompression: " << std::setprecision(3) << r.decompression_time_ms << " ms\n";
        }
    }


    const std::string& table_name() const { return table_name_; }
    const std::string& column_name() const { return column_name_; }

private:
    const std::string table_name_;
    const std::string column_name_;

    uint64_t uncompressed_size_;
    std::vector<AlgorithmResult> results_;
};


inline bool SaveResultsAsCSV(const std::vector<ExperimentResult>& experiments,
                             const std::filesystem::path& file_path)
{
    std::ofstream out(file_path, std::ios::binary);
    if (!out) return false;

    // Header
    out << "table,column,uncompressed_size,algorithm,compressed_size,compression_time_ms,decompression_time_ms\n";

    out << std::fixed << std::setprecision(3); // times to 3 decimals

    for (const auto& exp : experiments) {
        const auto& algos = exp.results();
        if (algos.empty()) {
            // still emit a row (without algo-specific fields) if you want; or skip
            continue;
        }
        for (const auto& ar : algos) {
            out << CSVEscape(exp.table_name()) << ','
                << CSVEscape(exp.column_name()) << ','
                << exp.uncompressedSize() << ','
                << CSVEscape(ToString(ar.algorithm)) << ','
                << ar.compressed_size << ','
                << ar.compression_time_ms << ','
                << ar.decompression_time_ms << '\n';
        }
    }
    return true;
}
