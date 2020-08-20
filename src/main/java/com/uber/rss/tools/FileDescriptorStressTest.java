package com.uber.rss.tools;

import com.uber.rss.util.SystemUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileDescriptorStressTest {

  private final String rootDir;
  private final int fileCount;

  public FileDescriptorStressTest(String rootDir, int fileCount) {
    this.rootDir = rootDir;
    this.fileCount = fileCount;
  }

  public void run() {
    int filesPerDir = 1000;
    int dirCount = (int)Math.ceil(((double)fileCount)/filesPerDir);
    System.out.println(String.format("Creating %s files with %s directories inside %s", fileCount, dirCount, rootDir));

    List<FileOutputStream> fileStreams = new ArrayList<>();

    try {
      for (int i = 0; i < dirCount; i++) {
        Path dirPath = Paths.get(rootDir, "dir" + i);
        dirPath.toFile().mkdirs();
        dirPath.toFile().deleteOnExit();
        System.out.println(String.format("Creating files under %s, current file descriptors: %s", dirPath.toAbsolutePath(), SystemUtils.getFileDescriptorCount()));
        for (int j = 0; j < filesPerDir; j++) {
          if (fileStreams.size() >= fileCount) {
            break;
          }
          Path filePath = Paths.get(dirPath.toString(), "file" + j);
          filePath.toFile().deleteOnExit();
          try {
            FileOutputStream stream = new FileOutputStream(filePath.toString(), true);
            fileStreams.add(stream);
            if (stream.getChannel().position() == 0) {
              stream.write(0);
              stream.flush();
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    } finally {
      System.out.println(String.format("Created %s files, current file descriptors: %s", fileStreams.size(), SystemUtils.getFileDescriptorCount()));
      fileStreams.forEach(t-> {
        try {
          t.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
  }

  public static void main(String[] args) throws Exception {
    String rootDir = "temp";
    if (args != null && args.length >= 1) {
      rootDir = args[0];
    }

    int fileCount = 1000;
    if (args != null && args.length >= 2) {
      fileCount = Integer.parseInt(args[1]);
    }

    FileDescriptorStressTest test = new FileDescriptorStressTest(rootDir, fileCount);
    test.run();
  }
}
