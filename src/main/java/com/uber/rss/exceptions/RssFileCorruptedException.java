package com.uber.rss.exceptions;

public class RssFileCorruptedException extends RssException {

    public RssFileCorruptedException(String message) {
        super(message);
    }

    public RssFileCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
