package com.uber.rss.exceptions;

/***
 * This exception is thrown when there is error with internal state in the shuffle server.
 */
public class RssInvalidStateException extends RssException {
    public RssInvalidStateException() {
    }

    public RssInvalidStateException(String message) {
        super(message);
    }

    public RssInvalidStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssInvalidStateException(Throwable cause) {
        super(cause);
    }

    public RssInvalidStateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
