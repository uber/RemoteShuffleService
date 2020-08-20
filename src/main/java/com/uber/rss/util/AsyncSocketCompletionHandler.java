package com.uber.rss.util;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.function.Consumer;

public class AsyncSocketCompletionHandler implements CompletionHandler<Integer, AsyncSocketState> {
    private Consumer<Throwable> exceptionCallback;
    
    public AsyncSocketCompletionHandler(Consumer<Throwable> exceptionCallback) {
        this.exceptionCallback = exceptionCallback;
    }
    
    @Override
    public void completed(Integer result, AsyncSocketState attachment) {
        ByteBuffer byteBuffer;
        
        synchronized (attachment) {
            byteBuffer = attachment.peekBuffer();
            if (byteBuffer == null) {
                return;
            }
        }
        
        if (byteBuffer.remaining() == 0) {
            synchronized (attachment) {
                ByteBuffer removed = attachment.removeBuffer();
                if (removed != byteBuffer) {
                    throw new RuntimeException("Removed buffer not same as expected, something is wrong!");
                }

                byteBuffer = attachment.peekBuffer();
                if (byteBuffer == null) {
                    return;
                }
            }

            attachment.getSocket().write(byteBuffer, attachment, this);
            return;
        }
        
        attachment.getSocket().write(byteBuffer, attachment, this);
    }

    @Override
    public void failed(Throwable exc, AsyncSocketState attachment) {
        this.exceptionCallback.accept(exc);
    }
}
