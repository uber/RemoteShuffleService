package com.uber.rss.exceptions;

public class RssInvalidDataException extends RssException {
    public RssInvalidDataException() {
    }

    public RssInvalidDataException(String message) {
        super(message);
    }

    public RssInvalidDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssInvalidDataException(Throwable cause) {
        super(cause);
    }

    public RssInvalidDataException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
