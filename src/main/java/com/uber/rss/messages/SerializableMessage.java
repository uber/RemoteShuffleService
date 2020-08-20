package com.uber.rss.messages;

import io.netty.buffer.ByteBuf;

public abstract class SerializableMessage {
    public abstract void serialize(ByteBuf buf);
}
