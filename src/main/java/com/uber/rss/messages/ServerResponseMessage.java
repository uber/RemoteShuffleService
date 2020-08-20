package com.uber.rss.messages;

/**
 * Base class used for all server response messages
 */
public abstract class ServerResponseMessage extends BaseMessage {
    protected byte status;

    public ServerResponseMessage() {
        this.status = MessageConstants.RESPONSE_STATUS_UNSPECIFIED;
    }

    public ServerResponseMessage(byte status) {
        this.status = status;
    }

    public byte getStatus() {
        return status;
    }
}
