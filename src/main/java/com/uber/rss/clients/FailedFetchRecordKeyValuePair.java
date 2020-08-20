package com.uber.rss.clients;

/***
 * This class is a marker to put into the record queue in async read client, so it knows 
 * there is an underlying failure.
 */
public class FailedFetchRecordKeyValuePair extends RecordKeyValuePair {
    private Throwable cause;
    
    public FailedFetchRecordKeyValuePair(Throwable cause) {
        super(null, null, 0L);
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }
}
