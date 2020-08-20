package com.uber.rss;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/***
 * This class contains integration tests (ITCase = Integration Test Case) which will be run by maven-failsafe-plugin.
 * Use "mvn clean verify" to run these integration tests.
 */
public class PackageJarFileITCase {

    @Test
    // Client jar file should only contain RSS files and shaded files, and should not contain 
    // unshaded dependency files. This test will check the files inside the jar file.
    public void checkClientJarFile() throws IOException {
        String jarFilePath = getClientJarPath();

        try (ZipFile zipFile = new ZipFile(jarFilePath)) {
            Enumeration<?> zipEntries = zipFile.entries();
            while (zipEntries.hasMoreElements()) {
                String fileName = ((ZipEntry) zipEntries.nextElement()).getName();

                if (fileName.startsWith("rss_shaded/")) {
                    continue;
                }
                
                if (fileName.equals("META-INF/")
                        || fileName.equals("META-INF/LICENSE.txt")) {
                    continue;
                }
                if (fileName.equals("META-INF/")
                        || fileName.equals("META-INF/MANIFEST.MF")) {
                    continue;
                }
                if (fileName.equals("META-INF/")
                        || fileName.equals("META-INF/NOTICE.txt")) {
                    continue;
                }

                if (fileName.equals("log4j-rss-debug.properties")
                        || fileName.equals("log4j-rss-prod.properties")
                        || fileName.equals("rss-build-info.properties")) {
                    continue;
                }
                
                if (fileName.equals("com/")
                        || fileName.equals("com/uber/")
                        || fileName.startsWith("com/uber/rss/")) {
                    continue;
                }

                if (fileName.equals("org/")
                        || fileName.equals("org/apache/")
                        || fileName.equals("org/apache/spark/")
                        || fileName.startsWith("org/apache/spark/shuffle/")) {
                    continue;
                }
                
                Assert.fail("Found unexpected file in jar file: " + fileName);
            }
        }
    }
    
    private String getClientJarPath() throws IOException {
        // Find jar file with largest size under target directory, which should be the packaged client jar file
        String agentJar = Files.list(Paths.get("target"))
                .max(Comparator.comparingLong(t -> {
                    try {
                        return Files.size(t);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }))
                .map(t->t.toString())
                .filter(t->t.endsWith("client.jar"))
                .get();
        System.out.println("client: " + agentJar);
        return agentJar;
    }
    
}
