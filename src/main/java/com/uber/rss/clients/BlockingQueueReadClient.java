package com.uber.rss.clients;

import com.uber.rss.common.DownloadServerVerboseInfo;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.metrics.M3Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/***
 * This class uses a background thread to read records and put records into a blocking queue.
 * Then it returns records from the blocking queue to its caller.
 */
public class BlockingQueueReadClient implements SingleServerReadClient {
    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueReadClient.class);
    
    private final SingleServerReadClient delegate;
    
    private final BlockingQueue<RecordKeyValuePair> recordQueue;
    private final long maxBlockingMillis;

    private volatile boolean stopped = false;

    public BlockingQueueReadClient(SingleServerReadClient delegate, int queueSize, long maxBlockingMillis) {
        this.delegate = delegate;

        this.recordQueue = new ArrayBlockingQueue<>(queueSize);
        logger.info(String.format("Created blocking queue with size: %s", queueSize));
        
        this.maxBlockingMillis = maxBlockingMillis;
    }

    @Override
    public DownloadServerVerboseInfo connect() {
        DownloadServerVerboseInfo serverVerboseInfo = delegate.connect();

        stopped = false;

        Thread thread = new Thread(() -> {
            logger.info("Started reading record in background thread");
            try {
                RecordKeyValuePair record = delegate.readRecord();
                while (!stopped && record != null) {
                    recordQueue.put(record);
                    record = delegate.readRecord();
                }
                recordQueue.put(new EofRecordKeyValuePair());
            } catch (Throwable ex) {
                M3Stats.addException(ex, this.getClass().getSimpleName());
                String logStr = String.format("Failed to read record, %s", delegate.toString());
                logger.warn(logStr, ex);
                recordQueue.clear();
                recordQueue.add(new FailedFetchRecordKeyValuePair(ex));
            }
            logger.info("Finished reading record in background thread");
        });

        thread.start();

        return serverVerboseInfo;
    }
    
    @Override
    public RecordKeyValuePair readRecord() {
        RecordKeyValuePair record;
        try {
            record = recordQueue.poll(maxBlockingMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RssException("Interrupted when polling record from blocking queue", e);
        }
        
        if (record == null) {
            throw new RssInvalidStateException(String.format(
                    "Timed out to poll record from blocking queue after waiting %s milliseconds",
                    maxBlockingMillis));
        }

        if (record instanceof FailedFetchRecordKeyValuePair) {
            throw new RssException("Failed to read record", ((FailedFetchRecordKeyValuePair)record).getCause());
        }
        
        if (record instanceof EofRecordKeyValuePair) {
            return null;
        }
        
        return record;
    }

    @Override
    public long getShuffleReadBytes() {
        return delegate.getShuffleReadBytes();
    }

    @Override
    public void close() {
        stopped = true;
        delegate.close();
    }

    @Override
    public String toString() {
        return "BlockingQueueReadClient{" +
            "delegate=" + delegate +
            '}';
    }
}
