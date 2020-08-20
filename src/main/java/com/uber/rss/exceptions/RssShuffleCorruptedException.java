package com.uber.rss.exceptions;

public class RssShuffleCorruptedException extends RssException {

    public RssShuffleCorruptedException(String message) {
        super(message);
    }

    public RssShuffleCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
