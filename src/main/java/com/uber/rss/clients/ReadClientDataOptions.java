package com.uber.rss.clients;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class ReadClientDataOptions {
  private final Collection<Long> latestTaskAttemptIds;
  private final long dataAvailablePollInterval;
  private final long dataAvailableWaitTime;

  public ReadClientDataOptions(Collection<Long> latestTaskAttemptIds, long dataAvailablePollInterval, long dataAvailableWaitTime) {
    this.latestTaskAttemptIds = Collections.unmodifiableCollection(new ArrayList(latestTaskAttemptIds));
    this.dataAvailablePollInterval = dataAvailablePollInterval;
    this.dataAvailableWaitTime = dataAvailableWaitTime;
  }

  public Collection<Long> getLatestTaskAttemptIds() {
    return latestTaskAttemptIds;
  }

  public long getDataAvailablePollInterval() {
    return dataAvailablePollInterval;
  }

  public long getDataAvailableWaitTime() {
    return dataAvailableWaitTime;
  }

  @Override
  public String toString() {
    return "WriteClientDataOptions{" +
        "latestTaskAttemptIds=" + latestTaskAttemptIds +
        ", dataAvailablePollInterval=" + dataAvailablePollInterval +
        ", dataAvailableWaitTime=" + dataAvailableWaitTime +
        '}';
  }
}
