package com.uber.rss.exceptions;

public class RssMissingShuffleWriteConfigException extends RssException {

    public RssMissingShuffleWriteConfigException(String message) {
        super(message);
    }

    public RssMissingShuffleWriteConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
