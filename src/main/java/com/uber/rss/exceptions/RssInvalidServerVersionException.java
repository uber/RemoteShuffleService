package com.uber.rss.exceptions;

public class RssInvalidServerVersionException extends RssException {
    public RssInvalidServerVersionException() {
    }

    public RssInvalidServerVersionException(String message) {
        super(message);
    }

    public RssInvalidServerVersionException(String message, Throwable cause) {
        super(message, cause);
    }

}
