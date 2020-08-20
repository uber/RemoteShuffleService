package com.uber.rss.storage;

import com.uber.rss.common.AppShuffleId;

import java.nio.file.Paths;

/***
 * Utility methods for shuffle files.
 */
public class ShuffleFileUtils {
    public static final int MAX_SPLITS = 10000;

    public static String getShuffleFileName(int shuffleId, int partitionId) {
        return String.format("shuffle_%s_p_%s.data", shuffleId, partitionId);
    }
    
    public static String getShuffleFilePath(String rootDir, 
                                          AppShuffleId appShuffleId,
                                          int partitionId) {
        String fileName = getShuffleFileName(
                appShuffleId.getShuffleId(), partitionId);
        String path = Paths.get(
                getAppShuffleDir(rootDir, appShuffleId.getAppId()), 
                appShuffleId.getAppAttempt(),
                fileName).toString();
        return path;
    }

    public static String getAppShuffleDir(String rootDir, String appId) {
        return Paths.get(rootDir, appId).toString();
    }
}
