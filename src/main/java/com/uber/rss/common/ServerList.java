package com.uber.rss.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ServerList {
  final private List<ServerDetail> serverList;

  @JsonCreator
  public ServerList(@JsonProperty("servers") Collection<ServerDetail> servers) {
    this.serverList = new ArrayList<>(servers);
  }

  @JsonCreator
  public ServerList(@JsonProperty("servers") ServerDetail[] servers) {
    this.serverList = Arrays.asList(servers);
  }

  @JsonProperty("servers")
  public List<ServerDetail> getSevers() {
    return new ArrayList<>(serverList);
  }

  @JsonIgnore
  public List<String> getSeverIds() {
    return serverList.stream().map(ServerDetail::getServerId).collect(Collectors.toList());
  }

  @JsonIgnore
  public ServerDetail getSeverDetail(String serverId) {
    for (ServerDetail entry: serverList) {
      if (entry.getServerId().equals(serverId)) {
        return entry;
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServerList that = (ServerList) o;
    return Objects.equals(serverList, that.serverList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serverList);
  }

  @Override
  public String toString() {
    return "ServerList{" +
        StringUtils.join(serverList, ',') +
        '}';
  }
}
