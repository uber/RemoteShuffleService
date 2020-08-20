package com.uber.rss.exceptions;

public class RssException extends RuntimeException {
    public RssException() {
    }

    public RssException(String message) {
        super(message);
    }

    public RssException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssException(Throwable cause) {
        super(cause);
    }

    public RssException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
