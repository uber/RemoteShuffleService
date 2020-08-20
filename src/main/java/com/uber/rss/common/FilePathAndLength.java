package com.uber.rss.common;

import java.util.Objects;

public class FilePathAndLength {
  private String path;
  private long length;

  public FilePathAndLength(String path, long length) {
    this.path = path;
    this.length = length;
  }

  public String getPath() {
    return path;
  }

  public long getLength() {
    return length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FilePathAndLength that = (FilePathAndLength) o;
    return length == that.length &&
        Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, length);
  }

  @Override
  public String toString() {
    return "FilePathAndLength{" +
        "path='" + path + '\'' +
        ", length=" + length +
        '}';
  }
}
