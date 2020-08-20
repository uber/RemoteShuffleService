package com.uber.rss.clients;

import com.uber.rss.common.ServerDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class refreshes new server connection by querying a server connection resolver. It also
 * updates the cache when getting a new connection string.
 */
public class ServerConnectionCacheUpdateRefresher implements ServerConnectionRefresher {

  private static final Logger logger = LoggerFactory.getLogger(ServerConnectionCacheUpdateRefresher.class);

  private final ServerConnectionStringResolver resolver;
  private final ServerConnectionStringCache cache;

  public ServerConnectionCacheUpdateRefresher(ServerConnectionStringResolver resolver, ServerConnectionStringCache cache) {
    this.resolver = resolver;
    this.cache = cache;
  }

  @Override
  public ServerDetail refreshConnection(ServerDetail serverDetail) {
    try {
      ServerDetail cached = cache.getServer(serverDetail);
      if (cached != null && Long.parseLong(cached.getRunningVersion()) > Long.parseLong(serverDetail.getRunningVersion())) {
        logger.info(String.format("Got newer server connection %s than %s from server cache, use that as refreshed value", cached, serverDetail));
        return cached;
      }
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to refresh server connection for %s from server cache, will try to query new connection string", serverDetail), ex);
    }

    ServerDetail refreshed = resolver.resolveConnection(serverDetail.getServerId());
    if (refreshed == null) {
      logger.warn(String.format("Got null from resolver when refresh new connection for %s", serverDetail));
      return serverDetail;
    } else {
      if (refreshed.equals(serverDetail)) {
        logger.info(String.format("Ignore refreshed connection %s for server %s due to unchanged value", refreshed, serverDetail));
        return serverDetail;
      } else {
        logger.info(String.format("Use refreshed connection %s for server %s and update server cache", refreshed, serverDetail));
        cache.updateServer(refreshed.getServerId(), refreshed);
        return refreshed;
      }
    }
  }
}
