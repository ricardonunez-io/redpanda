#include "compression/compression.h"

#include "compression/internal/gzip_compressor.h"
#include "compression/internal/lz4_frame_compressor.h"
#include "compression/internal/snappy_java_compressor.h"
#include "compression/internal/zstd_compressor.h"

namespace compression {
iobuf compressor::compress(const iobuf& io, type t) {
    switch (t) {
    case type::none:
        throw std::runtime_error("compressor: nothing to compress for 'none'");
    case type::gzip:
        return internal::gzip_compressor::compress(io);
    case type::snappy:
        return internal::snappy_java_compressor::compress(io);
    case type::lz4:
        return internal::lz4_frame_compressor::compress(io);
    case type::zstd:
        return internal::zstd_compressor::compress(io);
    }
    __builtin_unreachable();
}
iobuf compressor::uncompress(const iobuf& io, type t) {
    if (io.empty()) {
        throw std::runtime_error(
          fmt::format("Asked to decomrpess:{} an empty buffer:{}", (int)t, io));
    }
    switch (t) {
    case type::none:
        throw std::runtime_error(
          "compressor: nothing to uncompress for 'none'");
    case type::gzip:
        return internal::gzip_compressor::uncompress(io);
    case type::snappy:
        return internal::snappy_java_compressor::uncompress(io);
    case type::lz4:
        return internal::lz4_frame_compressor::uncompress(io);
    case type::zstd:
        return internal::zstd_compressor::uncompress(io);
    }
    __builtin_unreachable();
}

} // namespace compression
