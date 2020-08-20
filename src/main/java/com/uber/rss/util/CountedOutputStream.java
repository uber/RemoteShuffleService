package com.uber.rss.util;

import java.io.IOException;
import java.io.OutputStream;

public class CountedOutputStream extends OutputStream {
    private long writtenBytes = 0;

    private OutputStream underlyingStream;

    public CountedOutputStream(OutputStream underlyingStream) {
        this.underlyingStream = underlyingStream;
    }

    @Override
    public synchronized void write(int i) throws IOException {
        underlyingStream.write(i);
        writtenBytes++;
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        underlyingStream.write(b, off, len);
        writtenBytes += len;
    }

    @Override
    public synchronized void flush() throws IOException {
        underlyingStream.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        underlyingStream.close();
    }

    public synchronized long getWrittenBytes() {
        return writtenBytes;
    }
}
