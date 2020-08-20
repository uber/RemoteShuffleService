package com.uber.rss.exceptions;

public class RssServerDownException extends RssException {
    public RssServerDownException() {
    }

    public RssServerDownException(String message) {
        super(message);
    }
}
