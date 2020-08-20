package com.uber.rss.exceptions;

public class RssNonRecoverableException extends RssException {
    public RssNonRecoverableException() {
    }

    public RssNonRecoverableException(String message) {
        super(message);
    }

    public RssNonRecoverableException(String message, Throwable cause) {
        super(message, cause);
    }
}
