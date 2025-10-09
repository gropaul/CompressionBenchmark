#pragma once

#include <filesystem>
#include <fstream>
#include <utility>
#include <vector>
#include <string>
#include <iostream>
#include <iomanip>
#include "../utils/csv_utils.hpp"


struct TableConfig;

enum class AlgorithType {
    FSST,
    FSST12,
    OnPair,
    OnPair16,
    OnPairMini10,
    OnPairMini12,
    OnPairMini14,
    Dictionary,
    LZ4,
};


inline std::string ToString(AlgorithType algo) {
    switch (algo) {
        case AlgorithType::FSST: return "FSST";
        case AlgorithType::FSST12: return "FSST12";
        case AlgorithType::OnPair: return "OnPair";
        case AlgorithType::OnPair16: return "OnPair16";
        case AlgorithType::OnPairMini10: return "OnPairMini10";
        case AlgorithType::OnPairMini12: return "OnPairMini12";
        case AlgorithType::OnPairMini14: return "OnPairMini14";
        case AlgorithType::Dictionary: return "Dictionary";
        case AlgorithType::LZ4: return "LZ4";
    }
    return "Unknown";
}


struct CompressedSizeParts {
    // the data that the raw strings in the dictionary need can be complete strings for DICTIONARY COMPRESSION or substrings for FSST
    uint64_t size_dictionary_strings;

    // the length of the strings in the dictionary, usually bitpacked, in order to know where each string starts
    uint64_t size_dictionary_lengths;

    // the total size of the dictionary, including the string data and the lengths
    uint64_t size_dictionary;

    // the codes that represent the strings in the original data,
    // for FSST this these contain variable size elements
    // for Dictionary it is one code per string that is bitpacked. For LZ4 this will also contain the dictionary size
    uint64_t size_data_codes;

    // the lengths of the compressed raw strings
    // if there is only one code per string, this will be 0, otherwise
    uint64_t size_data_lengths;

    // the total size of the compressed data, including the codes and the lengths
    uint64_t size_data;
};

struct CompressedSizeInfo {
    uint64_t compressed_size;
    CompressedSizeParts parts;

    static CompressedSizeInfo FSST(uint64_t symbol_table_size, uint64_t data_codes_size, uint64_t data_lengths_size) {
        return CompressedSizeInfo{
            symbol_table_size + data_codes_size + data_lengths_size,
            {
                symbol_table_size, // size_dictionary_strings
                0, // size_dictionary_lengths
                symbol_table_size, // size_dictionary
                data_codes_size, // size_data_codes
                data_lengths_size, // size_data_lengths
                data_codes_size + data_lengths_size // size_data
            }
        };
    }

    static CompressedSizeInfo OnPair(uint64_t dictionary_strings_size, uint64_t dictionary_lengths_size,
                                     uint64_t data_codes_size, uint64_t data_lengths_size) {
        return CompressedSizeInfo{
            dictionary_strings_size + dictionary_lengths_size + data_codes_size + data_lengths_size,
            {
                dictionary_strings_size, // size_dictionary_strings
                dictionary_lengths_size, // size_dictionary_lengths
                dictionary_strings_size + dictionary_lengths_size, // size_dictionary
                data_codes_size, // size_data_codes
                data_lengths_size, // size_data_lengths
                data_codes_size + data_lengths_size // size_data
            }
        };
    }

    static CompressedSizeInfo Dictionary(uint64_t dictionary_strings_size, uint64_t dictionary_lengths_size,
                                         uint64_t data_codes_size) {
        const uint64_t dictionary_size = dictionary_strings_size + dictionary_lengths_size;
        constexpr uint64_t data_lengths_size = 0;
        // for dictionary compression, there is one code per string, so no lengths needed
        return CompressedSizeInfo{
            dictionary_size + data_codes_size,
            {
                dictionary_strings_size, // size_dictionary_strings
                dictionary_lengths_size, // size_dictionary_lengths
                dictionary_size, // size_dictionary
                data_codes_size, // size_data_codes
                data_lengths_size, // size_data_lengths
                data_codes_size + data_lengths_size // size_data
            }
        };
    }

    static CompressedSizeInfo LZ4(uint64_t data_codes_size, uint64_t data_lengths_size) {
        // the dictionary will be built from the data itself, so we do not count it here
        constexpr uint64_t dictionary_strings_size = 0;
        constexpr uint64_t dictionary_lengths_size = 0;
        constexpr uint64_t dictionary_size = 0;

        return CompressedSizeInfo{
            data_codes_size + data_lengths_size,
            {
                dictionary_strings_size, // size_dictionary_strings
                dictionary_lengths_size, // size_dictionary_lengths
                dictionary_size, // size_dictionary
                data_codes_size, // size_data_codes
                data_lengths_size, // size_data_lengths
                data_codes_size + data_lengths_size // size_data
            }
        };
    }
};


struct AlgorithmResult {
    AlgorithType algorithm;

    CompressedSizeInfo compressed_size_info;

    bool has_error;
    std::string error_message;

    double compression_time_ms;
    double decompression_time_ms_full;
    double decompression_time_ms_vector;
    double decompression_time_ms_random;

    uint64_t decompression_hash_full;
    uint64_t decompression_hash_vector;
    uint64_t decompression_hash_random;
};

inline AlgorithmResult MeanTimes(const std::vector<AlgorithmResult> &results) {
    if (results.empty()) {
        throw std::invalid_argument("MeanTimes: empty input");
    }

    AlgorithmResult mean = results[0]; // copy first as baseline

    double n = static_cast<double>(results.size());

    mean.compression_time_ms = 0.0;
    mean.decompression_time_ms_full = 0.0;
    mean.decompression_time_ms_vector = 0.0;
    mean.decompression_time_ms_random = 0.0;


    for (const auto &r: results) {
        mean.compression_time_ms += r.compression_time_ms;
        mean.decompression_time_ms_full += r.decompression_time_ms_full;
        mean.decompression_time_ms_vector += r.decompression_time_ms_vector;
        mean.decompression_time_ms_random += r.decompression_time_ms_random;
    }

    mean.compression_time_ms /= n;
    mean.decompression_time_ms_full /= n;
    mean.decompression_time_ms_vector /= n;
    mean.decompression_time_ms_random /= n;

    return mean;
}

class ExperimentResult {
public:
    explicit ExperimentResult(
        const uint64_t row_group_idx,
        const uint64_t uncompressed_size,
        const uint64_t uncompressed_size_strings,
        const uint64_t uncompressed_size_lengths,
        const uint64_t n_rows,
        const uint64_t n_rows_not_empty,
        std::string table_name,
        std::string column_name
    )
        : table_name_(std::move(table_name)), column_name_(std::move(column_name)), row_group_idx_(row_group_idx),
          n_rows_(n_rows), n_rows_not_empty_(n_rows_not_empty), uncompressed_size_(uncompressed_size),
          uncompressed_size_strings_(uncompressed_size_strings), uncompressed_size_lengths_(uncompressed_size_lengths) {
    }

    static ExperimentResult Empty() {
        return ExperimentResult(0, 0, 0, 0, 0, 0, "", "");
    }

    void setUncompressedSize(uint64_t size) { uncompressed_size_ = size; }
    uint64_t GetUncompressedSize() const { return uncompressed_size_; }
    uint64_t GetUncompressedSizeStrings() const { return uncompressed_size_strings_; }
    uint64_t GetUncompressedSizeLengths() const { return uncompressed_size_lengths_; }
    uint64_t GetNumRows() const { return n_rows_; }
    uint64_t GetNumRowsNotEmpty() const { return n_rows_not_empty_; }
    uint64_t GetRowGroupIdx() const { return row_group_idx_; }

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
            double factor = (r.compressed_size_info.compressed_size > 0)
                                ? static_cast<double>(uncompressed_size_) / r.compressed_size_info.compressed_size
                                : 0.0;


            os << "   - " << std::setw(7) << ToString(r.algorithm)
                    << ": " << r.compressed_size_info.compressed_size << " bytes"
                    << " (" << std::fixed << std::setprecision(2) << factor << "× smaller)"
                    << ", compression: " << std::setprecision(3) << r.compression_time_ms << " ms"
                    << ", decompression: " << std::setprecision(3) << r.decompression_time_ms_full << " ms\n";
        }
    }


    const std::string &table_name() const { return table_name_; }
    const std::string &column_name() const { return column_name_; }

private:
    const std::string table_name_;
    const std::string column_name_;

    const uint64_t row_group_idx_;
    const uint64_t n_rows_;
    const uint64_t n_rows_not_empty_;

    uint64_t uncompressed_size_;
    uint64_t uncompressed_size_strings_;
    uint64_t uncompressed_size_lengths_;
    std::vector<AlgorithmResult> results_;
};


inline bool SaveResultsAsCSV(const std::vector<ExperimentResult> &experiments,
                             const std::filesystem::path &file_path) {
    std::ofstream out(file_path, std::ios::binary);
    if (!out) return false;

    // Header
    out <<
            "table,column,row_group_idx,uncompressed_size,uncompressed_size_strings,uncompressed_size_lengths,"
            "n_rows,n_rows_not_empty,algorithm,compressed_size,"
            "compressed_size_dictionary_strings,compressed_size_dictionary_lengths,compressed_size_dictionary,size_data_codes,compressed_size_data_lengths,compressed_size_data,"
            "compression_time_ms,decompression_time_ms_full,decompression_time_ms_vector,decompression_time_ms_random,"
            "decompression_hash_full,decompression_hash_vector,decompression_hash_random,hasError,errorMessage\n";

    out << std::fixed << std::setprecision(6); // times to 3 decimals

    printf("Saving results to %s\n", file_path.string().c_str());
    for (const auto &exp: experiments) {
        const auto &algos = exp.results();
        if (algos.empty()) {
            printf("Skipping empty result for %s.%s\n", exp.table_name().c_str(), exp.column_name().c_str());
            // still emit a row (without algo-specific fields) if you want; or skip
            continue;
        }
        for (const auto &ar: algos) {
            out << CSVEscape(exp.table_name()) << ','
                    << CSVEscape(exp.column_name()) << ','
                    << exp.GetRowGroupIdx() << ','
                    << exp.GetUncompressedSize() << ','
                    << exp.GetUncompressedSizeStrings() << ','
                    << exp.GetUncompressedSizeLengths() << ','
                    << exp.GetNumRows() << ','
                    << exp.GetNumRowsNotEmpty() << ','
                    << CSVEscape(ToString(ar.algorithm)) << ','
                    << ar.compressed_size_info.compressed_size << ','
                    << ar.compressed_size_info.parts.size_dictionary_strings << ','
                    << ar.compressed_size_info.parts.size_dictionary_lengths << ','
                    << ar.compressed_size_info.parts.size_dictionary << ','
                    << ar.compressed_size_info.parts.size_data_codes << ','
                    << ar.compressed_size_info.parts.size_data_lengths << ','
                    << ar.compressed_size_info.parts.size_data << ','
                    << ar.compression_time_ms << ','
                    << ar.decompression_time_ms_full << ','
                    << ar.decompression_time_ms_vector << ','
                    << ar.decompression_time_ms_random << ','
                    << ar.decompression_hash_full << ','
                    << ar.decompression_hash_vector << ','
                    << ar.decompression_hash_random << ','
                    << ar.has_error << ','
                    << ar.error_message << '\n';
        }
    }
    return true;
}
