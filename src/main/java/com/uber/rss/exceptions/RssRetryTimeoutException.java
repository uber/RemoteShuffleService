package com.uber.rss.exceptions;

public class RssRetryTimeoutException extends RssException {
    public RssRetryTimeoutException() {
    }

    public RssRetryTimeoutException(String message) {
        super(message);
    }

    public RssRetryTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
