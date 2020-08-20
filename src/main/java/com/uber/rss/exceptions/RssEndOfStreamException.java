package com.uber.rss.exceptions;

public class RssEndOfStreamException extends RssException {
    public RssEndOfStreamException() {
    }

    public RssEndOfStreamException(String message) {
        super(message);
    }

    public RssEndOfStreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssEndOfStreamException(Throwable cause) {
        super(cause);
    }

    public RssEndOfStreamException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
