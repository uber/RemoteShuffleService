package com.uber.rss.handlers;

/***
 * This class contains response status and response message.
 * @param <T>
 */
public class ResponseStatusAndMessage<T> {
    private byte status;
    private T message;

    public ResponseStatusAndMessage(byte status, T message) {
        this.status = status;
        this.message = message;
    }

    public byte getStatus() {
        return status;
    }

    public T getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "ResponseStatusAndMessage{" +
                "status=" + status +
                ", message=" + message +
                '}';
    }
}
