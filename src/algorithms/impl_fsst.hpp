#pragma once

#include <stdexcept>

#include "fsst.h"
#include "interface.hpp"

enum class FsstVariant {
    FSST,
    FSST12
};



class FsstAlgorithm final : public ICompressionAlgorithm {
public:
    FsstAlgorithm() = default;

    [[nodiscard]] AlgorithType GetAlgorithmType() const override {
        return AlgorithType::FSST;
    }

    void Initialize(const ExperimentInput &input) override {
        compressed_lengths.resize(input.collector.Size());
        compressed_pointers.resize(input.collector.Size());

        compression_buffer_size = input.collector.TotalBytes() * 2 + 1000;
        compression_buffer = static_cast<uint8_t *>(malloc(compression_buffer_size));
    }

    idx_t GetDecompressionBufferSize(const idx_t decompressed_size) override {
        return decompressed_size + 32; // Small offset for safety
    }

    void CompressAll(const StringCollector &data) override {
        const StringCollector &collector = data;
        encoder = fsst_create(
            collector.Size(), /* IN: number of strings in batch to sample from. */
            collector.GetLengths().data(), /* IN: byte-lengths of the inputs */
            collector.GetPointers().data(), /* IN: string start pointers. */
            false
            /* IN: whether input strings are zero-terminated. If so, encoded strings are as well (i.e. symbol[0]=""). */
        );

        compressed_ready_ = true;

        fsst_compress(
            encoder, /* IN: encoder obtained from fsst_create(). */
            collector.Size(),
            collector.GetLengths().data(), /* IN: byte-lengths of the inputs */
            collector.GetPointers().data(),
            compression_buffer_size, /* IN: byte-length of output buffer. */
            compression_buffer, /* OUT: memory buffer to put the compressed strings in (one after the other). */
            compressed_lengths.data(), /* OUT: byte-lengths of the compressed strings. */
            compressed_pointers.data()
            /* OUT: output string start pointers. Will all point into [output,output+size). */
        );

        decoder = fsst_decoder(encoder);

        compressed_ready_ = true;
    }

    inline void DecompressAll(uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) throw std::logic_error("DecompressAll called before CompressAll/Benchmark");
        unsigned char *decompression_write_pointer = out;

        for (size_t index = 0; index < compressed_lengths.size(); index++) {
            const size_t decompressed_size = fsst_decompress(
                &decoder, /* IN: use this symbol table for compression. */
                compressed_lengths[index], /* IN: byte-length of compressed string. */
                compressed_pointers[index], /* IN: compressed string. */
                out_capacity, /* IN: byte-length of output buffer. */
                decompression_write_pointer /* OUT: memory buffer to put the decompressed string in. */
            );
            decompression_write_pointer += decompressed_size;
        }
    }

    inline idx_t DecompressOne(size_t index, uint8_t *out, size_t out_capacity) override {
        if (!compressed_ready_) throw std::logic_error("DecompressOne called before CompressAll/Benchmark");
        return fsst_decompress(
            &decoder, /* IN: use this symbol table for compression. */
            compressed_lengths[index], /* IN: byte-length of compressed string. */
            compressed_pointers[index], /* IN: compressed string. */
            out_capacity, /* IN: byte-length of output buffer. */
            out /* OUT: memory buffer to put the decompressed string in. */
        );
    }

    static size_t CalcSymbolTableSize(fsst_encoder_t *encoder) {
        // Correctly calculate decoder size by serialization
        unsigned char buffer[FSST_MAXHEADER];
        return fsst_export(encoder, buffer);
    }

    size_t CompressedSize() override {
        if (!compressed_ready_) throw std::logic_error("CompressedSize called before CompressAll/Benchmark");
        size_t total_compressed_size = 0;
        for (const unsigned long encoded_string_length: compressed_lengths) {
            total_compressed_size += encoded_string_length;
        }
        total_compressed_size += CalcSymbolTableSize(encoder);
        return total_compressed_size;
    }

    void Free() override {
        free(compression_buffer);
        fsst_destroy(encoder);
    }

private:
    bool compressed_ready_{false};
    fsst_encoder_t *encoder;
    fsst_decoder_t decoder;

    // buffer for the compressed data
    idx_t compression_buffer_size;
    uint8_t *compression_buffer;
    std::vector<size_t> compressed_lengths;
    std::vector<uint8_t *> compressed_pointers;
};
