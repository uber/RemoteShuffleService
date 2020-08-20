package com.uber.rss.clients;

public class ClientRetryOptions {
  private final long retryIntervalMillis;
  private final long retryMaxMillis;
  private final ServerConnectionRefresher retryConnectionResolver;

  public ClientRetryOptions(long retryIntervalMillis, long retryMaxMillis, ServerConnectionRefresher retryConnectionResolver) {
    this.retryIntervalMillis = retryIntervalMillis;
    this.retryMaxMillis = retryMaxMillis;
    this.retryConnectionResolver = retryConnectionResolver;
  }

  public long getRetryIntervalMillis() {
    return retryIntervalMillis;
  }

  public long getRetryMaxMillis() {
    return retryMaxMillis;
  }

  public ServerConnectionRefresher getRetryConnectionResolver() {
    return retryConnectionResolver;
  }

  @Override
  public String toString() {
    return "ClientRetryOptions{" +
        "retryIntervalMillis=" + retryIntervalMillis +
        ", retryMaxMillis=" + retryMaxMillis +
        ", retryConnectionResolver=" + retryConnectionResolver +
        '}';
  }
}
