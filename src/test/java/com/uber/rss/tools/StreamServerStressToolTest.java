package com.uber.rss.tools;

import org.testng.annotations.Test;

import java.nio.file.Files;

public class StreamServerStressToolTest {
    @Test
    public void runTool() throws Exception {
        String workDir = Files.createTempDirectory("StreamServerStressToolTest_").toString();

        StreamServerStressTool tool = new StreamServerStressTool();
        tool.setWorkDir(workDir);
        try {
            tool.run();
        } finally {
            tool.cleanup();
        }
    }
}
