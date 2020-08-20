package com.uber.rss.metrics;

import java.util.Objects;

public class ClientConnectMetricsKey {
    private String source;
    private String remote;

    public ClientConnectMetricsKey(String source, String remote) {
        this.source = source;
        this.remote = remote;
    }

    public String getSource() {
        return source;
    }

    public String getRemote() {
        return remote;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientConnectMetricsKey that = (ClientConnectMetricsKey) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(remote, that.remote);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, remote);
    }

    @Override
    public String toString() {
        return "ClientConnectMetricsKey{" +
                "source='" + source + '\'' +
                ", remote='" + remote + '\'' +
                '}';
    }
}
