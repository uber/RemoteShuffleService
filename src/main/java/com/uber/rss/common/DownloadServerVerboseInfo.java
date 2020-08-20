package com.uber.rss.common;

// TODO create a constructor to pass fields
public class DownloadServerVerboseInfo {
    private String runningVersion;
    private String id;

    private MapTaskCommitStatus mapTaskCommitStatus;

    public String getRunningVersion() {
        return runningVersion;
    }

    public void setRunningVersion(String runningVersion) {
        this.runningVersion = runningVersion;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public MapTaskCommitStatus getMapTaskCommitStatus() {
        return mapTaskCommitStatus;
    }

    public void setMapTaskCommitStatus(MapTaskCommitStatus mapTaskCommitStatus) {
        this.mapTaskCommitStatus = mapTaskCommitStatus;
    }

    @Override
    public String toString() {
        return "DownloadServerVerboseInfo{" +
                "runningVersion='" + runningVersion + '\'' +
                ", id='" + id + '\'' +
                ", mapTaskCommitStatus=" + mapTaskCommitStatus +
                '}';
    }
}
