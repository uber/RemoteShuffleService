package com.uber.rss.common;

import java.util.ArrayList;
import java.util.List;

public class ServerReplicationGroup {
  private final List<ServerDetail> servers;

  public ServerReplicationGroup(List<ServerDetail> servers) {
    this.servers = new ArrayList<>(servers);
  }

  public List<ServerDetail> getServers() {
    return new ArrayList<>(servers);
  }

  @Override
  public String toString() {
    return "ServerReplicationGroup{" +
        "servers=" + servers +
        '}';
  }
}
