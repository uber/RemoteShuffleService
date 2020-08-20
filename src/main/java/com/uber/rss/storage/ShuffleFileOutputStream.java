package com.uber.rss.storage;

import com.uber.rss.common.Compression;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.CountedOutputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStream;

/***
 * Local file based shuffle output stream.
 */
public class ShuffleFileOutputStream implements ShuffleOutputStream {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleFileOutputStream.class);
    
    private final String filePath;
    private final String compressionCodec;
    private OutputStream outputStream;
    private long initialFileSize = 0L;
    private CountedOutputStream internalCountedOutputStream;
    private final FileDescriptor fileDescriptor;

    public ShuffleFileOutputStream(File file, int bufferSize, String compressionCodec) {
        this.filePath = file.getAbsolutePath();
        this.compressionCodec = compressionCodec;
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            initialFileSize = fileOutputStream.getChannel().position();
            fileDescriptor = fileOutputStream.getFD();
            if (bufferSize == 0) {
                outputStream = fileOutputStream;
            } else {
                logger.debug(String.format("Creating BufferedOutputStream for %s", file.getAbsolutePath()));
                outputStream = new BufferedOutputStream(fileOutputStream, bufferSize);
            }

            internalCountedOutputStream = new CountedOutputStream(outputStream);

            if (compressionCodec != null && !compressionCodec.isEmpty()) {
                outputStream = Compression.compressStream(internalCountedOutputStream, compressionCodec);
                logger.debug(String.format("Switched to compressed stream with codec %s for file %s", compressionCodec, file));
            } else {
                outputStream = internalCountedOutputStream;
            }
        } catch (Throwable e) {
            M3Stats.addException(e, M3Stats.TAG_VALUE_SHUFFLE_OUTPUT_STREAM);
            throw new RssException(
                    "Failed to open or create writable file: " + this.filePath, e);
        }
    }

    @Override
    public void write(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return;
        }
        
        try {
            outputStream.write(bytes);
        } catch (Throwable e) {
            throw new RuntimeException(
                    "Failed to write file: " + filePath 
                            + ", number of bytes: " + bytes.length, e);
        }
    }

    @Override
    public void flush() {
        try {
            outputStream.flush();

            // TODO this is a quick hack to make sure lz4 stream writing finishing block, will refactor in future
            if (outputStream instanceof LZ4BlockOutputStream) {
                ((LZ4BlockOutputStream)outputStream).finish();
                outputStream = Compression.compressStream(internalCountedOutputStream, compressionCodec);
            }
        } catch (Throwable e) {
            throw new RuntimeException("Failed to flush file: " + filePath, e);
        }
    }

    @Override
    public void fsync() {
        try {
            flush();
            fileDescriptor.sync();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to fsync file: " + filePath, e);
        }
    }

    @Override
    public void close() {
        try {
            flush();
            outputStream.close();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to close file: " + filePath, e);
        }
    }

    @Override
    public String getLocation() {
        return filePath;
    }

    @Override
    public long getWrittenBytes() {
        return initialFileSize + internalCountedOutputStream.getWrittenBytes();
    }

    @Override
    public String toString() {
        return "ShuffleFileOutputStream{" +
                "filePath='" + filePath + '\'' +
                '}';
    }
}
