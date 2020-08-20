package com.uber.rss.util;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

// There are some methods in this class to convert int/long values to/from byte array.
// Theoretically we could create a ByteBuf inside those methods and use ByteBuf. Just to
// avoid creating such temporary ByteBuf instance, we decide to convert int/long values
// based on byte values. Those methods are similar to methods inside Netty HeapByteBufUtil.
// Unfortunately HeapByteBufUtil is not a public class, thus we could not use it directly.
// HeapByteBufUtil: https://github.com/netty/netty/blob/4.1/buffer/src/main/java/io/netty/buffer/HeapByteBufUtil.java.

public class ByteBufUtils {
    public static byte[] convertIntToBytes(int value) {
        byte[] bytes = new byte[Integer.BYTES];
        writeInt(bytes, 0, value);
        return bytes;
    }

    public static void writeLengthAndString(ByteBuf buf, String str) {
        if (str == null) {
            buf.writeInt(-1);
            return;
        }

        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
    
    public static String readLengthAndString(ByteBuf buf) {
        int length = buf.readInt();
        if (length == -1) {
            return null;
        }
        
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    public static void writeLengthAndByteBuffer(ByteBuf buf, ByteBuffer valueToWrite) {
        if (valueToWrite == null) {
            buf.writeInt(-1);
            return;
        }

        buf.writeInt(valueToWrite.remaining());
        buf.writeBytes(valueToWrite);
    }

    public static ByteBuffer readLengthAndByteBuffer(ByteBuf buf) {
        int length = buf.readInt();
        if (length == -1) {
            return null;
        }

        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }
    
    public static byte[] readBytes(ByteBuf buf) {
        // TODO a better implementation?
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }

    public static void readBytesToStream(ByteBuf buf, OutputStream stream) throws IOException {
        final int maxNumBytes = 64000;
        byte[] bytes = new byte[maxNumBytes];
        while (buf.readableBytes() > 0) {
            int numBytes = Math.min(buf.readableBytes(), maxNumBytes);
            buf.readBytes(bytes, 0, numBytes);
            stream.write(bytes, 0, numBytes);
        }
    }

    public static void writeInt(byte[] bytes, int index, int value) {
        bytes[index] = (byte) (value >>> 24);
        bytes[index + 1] = (byte) (value >>> 16);
        bytes[index + 2] = (byte) (value >>> 8);
        bytes[index + 3] = (byte) value;
    }

    public static void writeLong(byte[] bytes, int index, long value) {
        bytes[index] = (byte) (value >>> 56);
        bytes[index + 1] = (byte) (value >>> 48);
        bytes[index + 2] = (byte) (value >>> 40);
        bytes[index + 3] = (byte) (value >>> 32);
        bytes[index + 4] = (byte) (value >>> 24);
        bytes[index + 5] = (byte) (value >>> 16);
        bytes[index + 6] = (byte) (value >>> 8);
        bytes[index + 7] = (byte) value;
    }

    public static int readInt(byte[] bytes, int index) {
        return (bytes[index] & 0xff) << 24 |
            (bytes[index + 1] & 0xff) << 16 |
            (bytes[index + 2] & 0xff) <<  8 |
            bytes[index + 3] & 0xff;
    }

    public static long readLong(byte[] bytes, int index) {
        return ((long) bytes[index] & 0xff) << 56 |
            ((long) bytes[index + 1] & 0xff) << 48 |
            ((long) bytes[index + 2] & 0xff) << 40 |
            ((long) bytes[index + 3] & 0xff) << 32 |
            ((long) bytes[index + 4] & 0xff) << 24 |
            ((long) bytes[index + 5] & 0xff) << 16 |
            ((long) bytes[index + 6] & 0xff) <<  8 |
            (long) bytes[index + 7] & 0xff;
    }
}
