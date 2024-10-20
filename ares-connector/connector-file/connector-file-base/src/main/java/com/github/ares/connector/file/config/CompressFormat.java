package com.github.ares.connector.file.config;

import org.apache.orc.CompressionKind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.Serializable;

public enum CompressFormat implements Serializable {
    // text json orc parquet support
    LZO(".lzo", CompressionKind.LZO, CompressionCodecName.LZO),

    // orc and parquet support
    NONE("", CompressionKind.NONE, CompressionCodecName.UNCOMPRESSED),
    SNAPPY(".snappy", CompressionKind.SNAPPY, CompressionCodecName.SNAPPY),
    LZ4(".lz4", CompressionKind.LZ4, CompressionCodecName.LZ4),

    // only orc support
    ZLIB(".zlib", CompressionKind.ZLIB, CompressionCodecName.UNCOMPRESSED),

    // only parquet support
    GZIP(".gz", CompressionKind.NONE, CompressionCodecName.GZIP),
    BROTLI(".br", CompressionKind.NONE, CompressionCodecName.BROTLI),
    ZSTD(".zstd", CompressionKind.NONE, CompressionCodecName.ZSTD);

    private final String compressCodec;
    private final CompressionKind orcCompression;
    private final CompressionCodecName parquetCompression;

    CompressFormat(
            String compressCodec,
            CompressionKind orcCompression,
            CompressionCodecName parentCompression) {
        this.compressCodec = compressCodec;
        this.orcCompression = orcCompression;
        this.parquetCompression = parentCompression;
    }

    public String getCompressCodec() {
        return compressCodec;
    }

    public CompressionKind getOrcCompression() {
        return orcCompression;
    }

    public CompressionCodecName getParquetCompression() {
        return parquetCompression;
    }
}
