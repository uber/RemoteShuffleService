package com.uber.rss.exceptions;

public class RssQueueNotReadyException extends RssException {
    public RssQueueNotReadyException() {
    }

    public RssQueueNotReadyException(String message) {
        super(message);
    }

    public RssQueueNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }
}
