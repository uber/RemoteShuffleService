package com.uber.rss.exceptions;

public class RssServerBusyException extends RssException {
    public RssServerBusyException() {
    }

    public RssServerBusyException(String message) {
        super(message);
    }
}
