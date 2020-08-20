package com.uber.rss.metrics;

import java.util.Objects;

public class NettyServerSideMetricsKey {
    private String user;

    public NettyServerSideMetricsKey(String user) {
        this.user = user;
    }

    public String getUser() {
        return user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NettyServerSideMetricsKey that = (NettyServerSideMetricsKey) o;
        return Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user);
    }

    @Override
    public String toString() {
        return "NettyServerSideMetricsKey{" +
                "user='" + user + '\'' +
                '}';
    }
}
