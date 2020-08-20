package com.uber.rss.clients;

import com.uber.rss.common.ServerDetail;

/**
 * An interface to query server connection string by giving a server id.
 */
public interface ServerConnectionStringResolver {
  ServerDetail resolveConnection(String serverId);
}
