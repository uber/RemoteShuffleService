package com.uber.rss.exceptions;

public class RssFinishUploadException extends RssException {
    public RssFinishUploadException() {
    }

    public RssFinishUploadException(String message) {
        super(message);
    }

    public RssFinishUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}
