package com.uber.rss.exceptions;

public class RssRetryableSparkTaskException extends RssException {
    public RssRetryableSparkTaskException() {
    }

    public RssRetryableSparkTaskException(String message) {
        super(message);
    }

    public RssRetryableSparkTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
