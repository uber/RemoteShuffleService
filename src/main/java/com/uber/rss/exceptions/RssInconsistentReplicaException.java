package com.uber.rss.exceptions;

public class RssInconsistentReplicaException extends RssException {
    public RssInconsistentReplicaException() {
    }

    public RssInconsistentReplicaException(String message) {
        super(message);
    }

    public RssInconsistentReplicaException(String message, Throwable cause) {
        super(message, cause);
    }
}
