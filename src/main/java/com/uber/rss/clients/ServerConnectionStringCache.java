package com.uber.rss.clients;

import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerList;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class caches server connections by server ids. It also allow outside code to update
 * the cache.
 */
public class ServerConnectionStringCache {

  private final static ServerConnectionStringCache instance = new ServerConnectionStringCache();

  public static ServerConnectionStringCache getInstance() {
    return instance;
  }

  private ConcurrentHashMap<String, ServerDetail> servers = new ConcurrentHashMap<>();

  public ServerConnectionStringCache() {
  }

  public ServerDetail getServer(ServerDetail serverDetail) {
    ServerDetail cachedServerDetail = servers.get(serverDetail.getServerId());
    if (cachedServerDetail != null) {
      return cachedServerDetail;
    } else {
      return serverDetail;
    }
  }

  public ServerList getServerList(ServerList serverList) {
    return new ServerList(serverList.getSevers().stream().map(this::getServer).collect(Collectors.toList()));
  }

  public void updateServer(String serverId, ServerDetail serverDetail) {
    servers.put(serverId, serverDetail);
  }
}
