package com.uber.rss.exceptions;

public class RssMaxConnectionsException extends Exception {
    private String message;
    private int currentConnections = -1;
    private int maxConnections = -1;

    public RssMaxConnectionsException(int currentConnections, int maxConnections, String message) {
        super(message);
        this.message = message;
        this.currentConnections = currentConnections;
        this.maxConnections = maxConnections;
    }

    public int getCurrentConnections() {
        return currentConnections;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    @Override
    public String getMessage() {
        return String.format("%s, current: %s, max: %s", this.message, this.currentConnections, this.maxConnections);
    }

    @Override
    public String toString() {
        return "RssMaxConnectionsException{" +
                "currentConnections=" + currentConnections +
                ", maxConnections=" + maxConnections +
                "} " + super.toString();
    }
}
