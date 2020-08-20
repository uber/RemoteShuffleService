package com.uber.rss.exceptions;

public class RssInvalidServerIdException extends RssException {
    public RssInvalidServerIdException() {
    }

    public RssInvalidServerIdException(String message) {
        super(message);
    }

    public RssInvalidServerIdException(String message, Throwable cause) {
        super(message, cause);
    }
}
