package com.uber.rss.clients;

import com.uber.rss.common.DownloadServerVerboseInfo;

public interface SingleServerReadClient extends RecordReader {
    DownloadServerVerboseInfo connect();
}
