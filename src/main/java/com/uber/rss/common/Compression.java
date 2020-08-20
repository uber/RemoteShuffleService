package com.uber.rss.common;

import com.uber.rss.exceptions.RssUnsupportedCompressionException;
import net.jpountz.lz4.*;
import net.jpountz.xxhash.XXHashFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Checksum;

public class Compression {
    private static final Logger logger = LoggerFactory.getLogger(Compression.class);

    public final static String COMPRESSION_CODEC_LZ4 = "lz4";

    private static final int defaultLz4BlockSize = 65536;
    private static final int defaultLz4ChecksumSeed = -1756908916;

    public static OutputStream compressStream(OutputStream stream, String codec) {
        if (codec == null || codec.isEmpty()) {
            return stream;
        }

        if (codec.equals(Compression.COMPRESSION_CODEC_LZ4)) {
            LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
            Checksum defaultLz4Checksum = XXHashFactory.fastestInstance().newStreamingHash32(defaultLz4ChecksumSeed).asChecksum();
            return new LZ4BlockOutputStream(stream, defaultLz4BlockSize, compressor, defaultLz4Checksum, true);
        } else {
            throw new RssUnsupportedCompressionException(String.format("Unsupported compression codec: %s", codec));
        }
    }

    public static InputStream decompressStream(InputStream stream, String codec) {
        if (codec == null || codec.isEmpty()) {
            return stream;
        }

        if (codec.equals(Compression.COMPRESSION_CODEC_LZ4)) {
            LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();
            Checksum defaultLz4Checksum = XXHashFactory.fastestInstance().newStreamingHash32(defaultLz4ChecksumSeed).asChecksum();
            return new LZ4BlockInputStream(stream, decompressor, defaultLz4Checksum, false);
        } else {
            throw new RssUnsupportedCompressionException(String.format("Unsupported compression codec: %s", codec));
        }
    }
}
