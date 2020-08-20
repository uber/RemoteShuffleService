package com.uber.rss.testutil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class NettyMemoryExperiments {
    
    private static void testMemorySize() {
        long allocatedBytes = 0;
        long totalBytes = 4L * 1024 * 1024 * 1024;
        while (allocatedBytes < totalBytes) {
            int bufferSize = 4096;
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(bufferSize);
            allocatedBytes += bufferSize;
            System.out.println(String.format(
                    "Allocated bytes: %s, Netty heap memory: %s, Netty directy memory: %s",
                    allocatedBytes,
                    PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory(),
                    PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory()));
            buf.release();
        }
        
    }
    
    public static void main(String[] args) {
        testMemorySize();
    }
}
