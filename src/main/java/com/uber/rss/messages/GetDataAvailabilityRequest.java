package com.uber.rss.messages;

import io.netty.buffer.ByteBuf;

/***
 * Message sent by the client to check shuffle data availability
 */
public class GetDataAvailabilityRequest extends BaseMessage {

    public GetDataAvailabilityRequest() {
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_GetDataAvailabilityRequest;
    }

    @Override
    public void serialize(ByteBuf buf) {
    }

    public static GetDataAvailabilityRequest deserialize(ByteBuf buf) {
        return new GetDataAvailabilityRequest();
    }

    @Override
    public String toString() {
        return "GetDataAvailabilityRequest{" +
                '}';
    }
}
