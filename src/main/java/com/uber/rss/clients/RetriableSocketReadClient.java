package com.uber.rss.clients;

import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.DownloadServerVerboseInfo;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.exceptions.RssNetworkException;
import com.uber.rss.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class RetriableSocketReadClient implements SingleServerReadClient {
  private static final Logger logger = LoggerFactory.getLogger(RetriableSocketReadClient.class);

  private final ClientRetryOptions retryOptions;
  private final Supplier<ServerIdAwareSocketReadClient> retryClientCreator;

  private ServerIdAwareSocketReadClient delegate;

  public RetriableSocketReadClient(ServerDetail serverDetail,
                                   int timeoutMillis,
                                   ClientRetryOptions retryOptions,
                                   boolean compressed,
                                   int queueSize,
                                   String user,
                                   AppShufflePartitionId appShufflePartitionId,
                                   ReadClientDataOptions dataOptions) {
    this.retryOptions = retryOptions;

    delegate = new ServerIdAwareSocketReadClient(serverDetail,
        timeoutMillis,
        compressed,
        queueSize,
        user,
        appShufflePartitionId,
        dataOptions.getLatestTaskAttemptIds(),
        dataOptions.getDataAvailablePollInterval(),
        dataOptions.getDataAvailableWaitTime());

    this.retryClientCreator = () -> {
      ServerDetail retryServerDetail = retryOptions.getRetryConnectionResolver().refreshConnection(serverDetail);
      return new ServerIdAwareSocketReadClient(retryServerDetail,
            timeoutMillis,
            compressed,
            queueSize,
            user,
            appShufflePartitionId,
          dataOptions.getLatestTaskAttemptIds(),
          dataOptions.getDataAvailablePollInterval(),
          dataOptions.getDataAvailableWaitTime());
    };
  }

  @Override
  public DownloadServerVerboseInfo connect() {
    long startTime = System.currentTimeMillis();
    RssNetworkException lastException;
    do {
      try {
        return delegate.connect();
      } catch (RssNetworkException ex) {
        lastException = ex;
        logger.warn(String.format("Failed to connect to server: %s", delegate), ex);
        closeDelegate();
        long retryRemainingMillis = startTime + retryOptions.getRetryMaxMillis() - System.currentTimeMillis();
        if (retryRemainingMillis <= 0) {
          break;
        } else {
          delegate = retryClientCreator.get();
          long waitMillis = Math.min(retryOptions.getRetryIntervalMillis(), retryRemainingMillis);
          logger.info(String.format("Waiting %s milliseconds (total retry milliseconds: %s, remaining milliseconds: %s) and retry to connect to server: %s",
              waitMillis, retryOptions.getRetryMaxMillis(), retryRemainingMillis, delegate));
          ThreadUtils.sleep(waitMillis);
        }
        continue;
      }
    } while (System.currentTimeMillis() <= startTime + retryOptions.getRetryMaxMillis());
    throw lastException;
  }

  @Override
  public RecordKeyValuePair readRecord() {
    return delegate.readRecord();
  }

  @Override
  public long getShuffleReadBytes() {
    return delegate.getShuffleReadBytes();
  }

  @Override
  public void close() {
    closeDelegate();
  }

  @Override
  public String toString() {
    return "RetriableSocketReadClient{" +
        "retryOptions=" + retryOptions +
        ", delegate=" + delegate +
        '}';
  }

  private void closeDelegate() {
    try {
      delegate.close();
    } catch (Throwable ex) {
      logger.warn("Failed to close underlying client: " + delegate, ex);
    }
  }
}
