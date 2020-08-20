package com.uber.rss.metadata;

public class ZooKeeperServerNodeData {
    private String runningVersion;
    private String connectionString;

    public String getRunningVersion() {
        return runningVersion;
    }

    public void setRunningVersion(String runningVersion) {
        this.runningVersion = runningVersion;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public String toString() {
        return "ZooKeeperServerNodeData{" +
                "runningVersion='" + runningVersion + '\'' +
                ", connectionString='" + connectionString + '\'' +
                '}';
    }
}
