package com.uber.rss.exceptions;

public class RssNoActiveReadClientException extends RssException {
    public RssNoActiveReadClientException() {
    }

    public RssNoActiveReadClientException(String message) {
        super(message);
    }

    public RssNoActiveReadClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
