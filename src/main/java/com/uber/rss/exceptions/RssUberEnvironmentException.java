package com.uber.rss.exceptions;

public class RssUberEnvironmentException extends RssException {
    public RssUberEnvironmentException() {
    }

    public RssUberEnvironmentException(String message) {
        super(message);
    }

    public RssUberEnvironmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
