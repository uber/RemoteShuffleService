package com.uber.rss.exceptions;

public class RssNoServerAvailableException extends RssException {
    public RssNoServerAvailableException() {
    }

    public RssNoServerAvailableException(String message) {
        super(message);
    }
}
