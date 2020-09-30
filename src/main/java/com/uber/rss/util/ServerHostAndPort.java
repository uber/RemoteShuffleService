package com.uber.rss.util;

import com.uber.rss.exceptions.RssInvalidDataException;

import java.util.Objects;

public class ServerHostAndPort {
  private final String host;
  private final int port;

  public static ServerHostAndPort fromString(String str) {
    return new ServerHostAndPort(str);
  }

  public ServerHostAndPort(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public ServerHostAndPort(String str) {
    if (str == null) {
      this.host = null;
      this.port = 0;
    } else {
      String[] strArray = str.split(":");
      if (strArray.length == 1) {
        this.host = strArray[0];
        this.port = 0;
      } else if (strArray.length == 2) {
        this.host = strArray[0];
        try {
          this.port = Integer.parseInt(strArray[1].trim());
        } catch (Throwable ex) {
          throw new RssInvalidDataException(String.format("Invalid host and port string: %s", str));
        }
      } else {
        throw new RssInvalidDataException(String.format("Invalid host and port string: %s", str));
      }
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServerHostAndPort that = (ServerHostAndPort) o;
    return port == that.port &&
        Objects.equals(host, that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public String toString() {
    return String.format("%s:%s", host, port);
  }
}
