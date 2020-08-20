package com.uber.rss.metrics;

import java.util.Objects;

public class MetadataClientMetricsKey {
    private String client;
    private String operation;

    public MetadataClientMetricsKey(String client, String operation) {
        this.client = client;
        this.operation = operation;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetadataClientMetricsKey that = (MetadataClientMetricsKey) o;
        return Objects.equals(client, that.client) &&
                Objects.equals(operation, that.operation);
    }

    @Override
    public int hashCode() {

        return Objects.hash(client, operation);
    }

    @Override
    public String toString() {
        return "MetadataClientMetricsKey{" +
                "client='" + client + '\'' +
                ", operation='" + operation + '\'' +
                '}';
    }
}
