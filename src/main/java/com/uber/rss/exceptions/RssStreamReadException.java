package com.uber.rss.exceptions;

public class RssStreamReadException extends RssException {
    public RssStreamReadException() {
    }

    public RssStreamReadException(String message) {
        super(message);
    }

    public RssStreamReadException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssStreamReadException(Throwable cause) {
        super(cause);
    }

    public RssStreamReadException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
