package com.uber.rss.exceptions;

public class RssUnsupportedCompressionException extends RssException {
    public RssUnsupportedCompressionException() {
    }

    public RssUnsupportedCompressionException(String message) {
        super(message);
    }

    public RssUnsupportedCompressionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssUnsupportedCompressionException(Throwable cause) {
        super(cause);
    }

    public RssUnsupportedCompressionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
