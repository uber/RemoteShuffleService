package com.uber.rss.util;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AsyncSocketState {
    private AsynchronousSocketChannel socket;
    private ConcurrentLinkedQueue<ByteBuffer> byteBuffers = new ConcurrentLinkedQueue<>();

    public AsyncSocketState(AsynchronousSocketChannel socket) {
        this.socket = socket;
    }

    public AsynchronousSocketChannel getSocket() {
        return socket;
    }
    
    public void addBuffer(ByteBuffer byteBuffer) {
        byteBuffers.add(byteBuffer);
    }

    public ByteBuffer peekBuffer() {
        if (byteBuffers.isEmpty()) {
            return null;
        }
        
        return byteBuffers.peek();
    }
    
    public ByteBuffer removeBuffer() {
        return byteBuffers.poll();
    }

    @Override
    public String toString() {
        return "AsyncSocketState{" +
                "byteBuffers=" + byteBuffers.size() +
                '}';
    }
}
