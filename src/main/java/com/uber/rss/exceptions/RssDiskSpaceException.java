package com.uber.rss.exceptions;

public class RssDiskSpaceException extends RssException {
    public RssDiskSpaceException() {
    }

    public RssDiskSpaceException(String message) {
        super(message);
    }

    public RssDiskSpaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssDiskSpaceException(Throwable cause) {
        super(cause);
    }

    public RssDiskSpaceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
