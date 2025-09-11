#pragma once

#include "../models/string_collection.hpp"
#include "../models/compression_result.hpp"



inline void PrintSymbolTableSize(fsst_encoder_t *encoder) {
    unsigned char buffer[FSST_MAXHEADER];
    const size_t n_bytes = fsst_export(encoder, buffer);
    printf("Symbol table size: %zu bytes\n", n_bytes);

    for (size_t i = 0; i < n_bytes; i++) {
        printf("%02X ", buffer[i]); // hex output
    }
    printf("\n");
}


inline size_t CalcSymbolTableSize(fsst_encoder_t *encoder) {
    // Correctly calculate decoder size by serialization
    unsigned char buffer[FSST_MAXHEADER];
    return fsst_export(encoder, buffer);
}

inline AlgorithmResult CompressFSST(const StringCollector &collector) {
    using clock = std::chrono::high_resolution_clock;

    // Compression outputs
    std::vector<size_t> compressed_lengths(collector.Size());
    std::vector<uint8_t *> compressed_pointers(collector.Size());

    const auto t0 = clock::now();

    fsst_encoder_t *encoder = fsst_create(
        collector.Size(), /* IN: number of strings in batch to sample from. */
        collector.GetLengths().data(), /* IN: byte-lengths of the inputs */
        collector.GetPointers().data(), /* IN: string start pointers. */
        false /* IN: whether input strings are zero-terminated. If so, encoded strings are as well (i.e. symbol[0]=""). */
    );

    // Calculate worst-case output size (2 * input) + datastructure overhead
    size_t compression_buffer_size = collector.TotalBytes() * 2 + 1000;
    auto *compression_buffer = static_cast<uint8_t *>(malloc(compression_buffer_size));

    fsst_compress(
        encoder, /* IN: encoder obtained from fsst_create(). */
        collector.Size(),
        collector.GetLengths().data(), /* IN: byte-lengths of the inputs */
        collector.GetPointers().data(),
        compression_buffer_size, /* IN: byte-length of output buffer. */
        compression_buffer, /* OUT: memory buffer to put the compressed strings in (one after the other). */
        compressed_lengths.data(), /* OUT: byte-lengths of the compressed strings. */
        compressed_pointers.data() /* OUT: output string start pointers. Will all point into [output,output+size). */
    );

    size_t total_compressed_size = 0;
    for (const unsigned long encoded_string_length: compressed_lengths) {
        total_compressed_size += encoded_string_length;
    }
    total_compressed_size += CalcSymbolTableSize(encoder);

    auto t1 = clock::now();

    fsst_decoder_t decoder = fsst_decoder(encoder);

    const size_t decompression_buffer_size = collector.TotalBytes();
    auto *decompression_buffer = static_cast<uint8_t *>(malloc(decompression_buffer_size));
    unsigned char *decompression_write_pointer = decompression_buffer;

    for (size_t index = 0; index < collector.Size(); index++) {
        const size_t decompressed_size = fsst_decompress(
            &decoder, /* IN: use this symbol table for compression. */
            compressed_lengths[index], /* IN: byte-length of compressed string. */
            compressed_pointers[index], /* IN: compressed string. */
            decompression_buffer_size, /* IN: byte-length of output buffer. */
            decompression_write_pointer /* OUT: memory buffer to put the decompressed string in. */
        );
        decompression_write_pointer += decompressed_size;
    }

    auto t2 = clock::now();

    auto compression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    auto decompression_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();


    free(decompression_buffer);
    free(compression_buffer);
    fsst_destroy(encoder);


    return {
        CompressionAlgorithm::FSST,
        total_compressed_size,
        compression_duration_ns / 1e6,
        decompression_duration_ns / 1e6
    };
}
