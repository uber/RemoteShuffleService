package com.uber.rss.exceptions;

public class RssShuffleStageNotStartedException extends RssException {

    public RssShuffleStageNotStartedException(String message) {
        super(message);
    }

    public RssShuffleStageNotStartedException(String message, Throwable cause) {
        super(message, cause);
    }
}
