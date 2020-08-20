package com.uber.rss.exceptions;

public class RssShuffleDataNotAvailableException extends RssException {

    public RssShuffleDataNotAvailableException(String message) {
        super(message);
    }

    public RssShuffleDataNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
