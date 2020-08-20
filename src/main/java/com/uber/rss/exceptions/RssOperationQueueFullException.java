package com.uber.rss.exceptions;

public class RssOperationQueueFullException extends RssException {
    public RssOperationQueueFullException() {
    }

    public RssOperationQueueFullException(String message) {
        super(message);
    }

    public RssOperationQueueFullException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssOperationQueueFullException(Throwable cause) {
        super(cause);
    }

    public RssOperationQueueFullException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
