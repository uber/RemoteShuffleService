/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.rss.common;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.exceptions.RssUnsupportedCompressionException;
import net.jpountz.lz4.*;
import net.jpountz.xxhash.XXHashFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Checksum;

public class Compression {
    private static final Logger logger = LoggerFactory.getLogger(Compression.class);

    public final static String COMPRESSION_CODEC_LZ4 = "lz4";
    public final static String COMPRESSION_CODEC_ZSTD = "zstd";

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
        } else if (codec.equals(Compression.COMPRESSION_CODEC_ZSTD)) {
            try {
                return new ZstdOutputStream(stream);
            } catch (IOException e) {
                throw new RssException("Failed to create ZstdOutputStream", e);
            }
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
        } else if (codec.equals(Compression.COMPRESSION_CODEC_ZSTD)) {
            try {
                return new ZstdInputStream(stream);
            } catch (IOException e) {
                throw new RssException("Failed to create ZstdInputStream", e);
            }
        } else {
            throw new RssUnsupportedCompressionException(String.format("Unsupported compression codec: %s", codec));
        }
    }
}
