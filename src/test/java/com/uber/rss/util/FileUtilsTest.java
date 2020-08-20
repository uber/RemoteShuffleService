package com.uber.rss.util;

import com.uber.rss.exceptions.RssDiskSpaceException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class FileUtilsTest {
    
    @Test
    public void cleanupOldFiles() throws IOException {
        FileUtils.cleanupOldFiles("/not_existing_directory/just_for_test", System.currentTimeMillis());

        // create temp directory as root directory
        Path tempDir = Files.createTempDirectory("FileUtilsTest_");
        tempDir.toFile().deleteOnExit();
        Assert.assertTrue(tempDir.toFile().exists());

        long cutoffTime = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);

        // clean up directory when there is no file
        FileUtils.cleanupOldFiles(tempDir.toString(), cutoffTime);
        Assert.assertTrue(tempDir.toFile().exists());
        Assert.assertEquals(tempDir.toFile().list().length, 0);

        // create file and child directory
        Path file1 = Paths.get(tempDir.toString(), "file1.txt");
        Files.write(file1, "test data".getBytes(StandardCharsets.UTF_8));

        Path dir1 = Paths.get(tempDir.toString(), "dir1");
        dir1.toFile().mkdirs();

        Path file2 = Paths.get(dir1.toString(), "file2.txt");
        Files.write(file2, "test data".getBytes(StandardCharsets.UTF_8));

        Assert.assertEquals(tempDir.toFile().list().length, 2);

        // clean up directory (now there is file and child directory) with very old cutoff time, should not delete files
        FileUtils.cleanupOldFiles(tempDir.toString(), System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
        Assert.assertTrue(tempDir.toFile().exists());
        Assert.assertEquals(tempDir.toFile().list().length, 2);

        // clean up directory (now there is file and child directory) with current cutoff time, should delete files
        FileUtils.cleanupOldFiles(tempDir.toString(), cutoffTime);
        Assert.assertTrue(tempDir.toFile().exists());
        Assert.assertEquals(tempDir.toFile().list().length, 0);
    }

    @Test
    public void checkDiskFreeSpace() {
        FileUtils.checkDiskFreeSpace(1, 1);
    }

    @Test(expectedExceptions = {RssDiskSpaceException.class})
    public void checkDiskFreeSpace_NotEnoughTotalSpace() {
        FileUtils.checkDiskFreeSpace(Long.MAX_VALUE, 1);
    }

    @Test(expectedExceptions = {RssDiskSpaceException.class})
    public void checkDiskFreeSpace_NotEnoughFreeSpace() {
        FileUtils.checkDiskFreeSpace(1, Long.MAX_VALUE);
    }

    @Test
    public void getFileStoreUsableSpace() {
        Assert.assertTrue(FileUtils.getFileStoreUsableSpace() > 1);
    }
}
