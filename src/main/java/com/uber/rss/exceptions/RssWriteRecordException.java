package com.uber.rss.exceptions;

public class RssWriteRecordException extends RssException {
    public RssWriteRecordException() {
    }

    public RssWriteRecordException(String message) {
        super(message);
    }

    public RssWriteRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
