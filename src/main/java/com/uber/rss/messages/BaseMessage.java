package com.uber.rss.messages;

public abstract class BaseMessage extends SerializableMessage {
    public abstract int getMessageType();
}
