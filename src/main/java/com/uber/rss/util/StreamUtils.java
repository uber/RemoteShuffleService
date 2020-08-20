package com.uber.rss.util;

import com.uber.rss.exceptions.RssEndOfStreamException;
import com.uber.rss.exceptions.RssStreamReadException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;

public class StreamUtils {
    
    /***
     * Read given number of bytes from the stream.
     * Return null means end of stream.
     * @param stream
     * @param numBytes
     * @return byte array, returning null means end of stream
     */
    @Nullable
    public static byte[] readBytes(InputStream stream, int numBytes) {
        if (numBytes == 0) {
            return new byte[0];
        }

        byte[] result = new byte[numBytes];
        int readBytes = 0;
        while (readBytes < numBytes) {
            int numBytesToRead = numBytes - readBytes;
            int count;
            try {
                count = stream.read(result, readBytes, numBytesToRead);
            } catch (IOException e) {
                throw new RssStreamReadException("Failed to read data", e);
            }
            if (count == -1) {
                if (readBytes == 0) {
                    return null;
                } else {
                    throw new RssEndOfStreamException(String.format(
                            "Unexpected end of stream, already read bytes: %s, remaining bytes to read: %s ",
                            readBytes,
                            numBytesToRead));
                }
            }
            readBytes += count;
        }

        return result;
    }
}
