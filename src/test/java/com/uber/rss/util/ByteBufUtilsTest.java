package com.uber.rss.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Int;

public class ByteBufUtilsTest {
    
    @Test
    public void convertIntToBytes() {
        byte[] bytes = ByteBufUtils.convertIntToBytes(1);
        Assert.assertEquals(bytes.length, 4);
        Assert.assertEquals(bytes, new byte[] {0, 0, 0, 1});

        bytes = ByteBufUtils.convertIntToBytes(Int.MinValue());
        Assert.assertEquals(bytes.length, 4);

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(4);
        buf.writeBytes(bytes);

        Assert.assertEquals(buf.readInt(), Int.MinValue());

        bytes = ByteBufUtils.convertIntToBytes(Int.MaxValue());
        Assert.assertEquals(bytes.length, 4);

        buf = PooledByteBufAllocator.DEFAULT.buffer(4);
        buf.writeBytes(bytes);

        Assert.assertEquals(buf.readInt(), Int.MaxValue());

        buf.release();
    }
}
