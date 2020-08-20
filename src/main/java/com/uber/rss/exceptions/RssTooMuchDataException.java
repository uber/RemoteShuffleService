package com.uber.rss.exceptions;

public class RssTooMuchDataException extends RssException {
    public RssTooMuchDataException() {
    }

    public RssTooMuchDataException(String message) {
        super(message);
    }
}
