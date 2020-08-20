package com.uber.rss.exceptions;

public class RssNetworkException extends RssException {
    public RssNetworkException() {
    }

    public RssNetworkException(String message) {
        super(message);
    }

    public RssNetworkException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssNetworkException(Throwable cause) {
        super(cause);
    }

    public RssNetworkException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
