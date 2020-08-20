package com.uber.rss;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class StreamServerConfigTest {
    @Test
    public void buildFromArgs() throws IOException {
        String[] args = new String[]{};
        StreamServerConfig config = StreamServerConfig.buildFromArgs(args);
        Assert.assertEquals(config.getCluster(), "default");

        args = new String[]{"-cluster", "staging"};
        config = StreamServerConfig.buildFromArgs(args);
        Assert.assertEquals(config.getCluster(), "staging");
    }
}
