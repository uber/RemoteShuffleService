package com.uber.rss.exceptions;

/***
 * This exception is thrown when a same Spark application task attempt connects to shuffle server multiple times.
 */
public class RssDuplicateAppTaskAttemptException extends RssException {
    public RssDuplicateAppTaskAttemptException() {
    }

    public RssDuplicateAppTaskAttemptException(String message) {
        super(message);
    }

    public RssDuplicateAppTaskAttemptException(String message, Throwable cause) {
        super(message, cause);
    }

    public RssDuplicateAppTaskAttemptException(Throwable cause) {
        super(cause);
    }

    public RssDuplicateAppTaskAttemptException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
