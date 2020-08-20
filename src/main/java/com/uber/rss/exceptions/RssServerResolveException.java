package com.uber.rss.exceptions;

public class RssServerResolveException extends RssException {
    public RssServerResolveException() {
    }

    public RssServerResolveException(String message) {
        super(message);
    }

    public RssServerResolveException(String message, Throwable cause) {
        super(message, cause);
    }
}
