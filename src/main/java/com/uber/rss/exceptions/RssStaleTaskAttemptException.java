package com.uber.rss.exceptions;

public class RssStaleTaskAttemptException extends RssException {

    public RssStaleTaskAttemptException(String message) {
        super(message);
    }

    public RssStaleTaskAttemptException(String message, Throwable cause) {
        super(message, cause);
    }
}
