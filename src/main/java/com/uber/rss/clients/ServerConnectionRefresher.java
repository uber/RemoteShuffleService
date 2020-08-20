package com.uber.rss.clients;

import com.uber.rss.common.ServerDetail;

public interface ServerConnectionRefresher {
  ServerDetail refreshConnection(ServerDetail serverDetail);
}
