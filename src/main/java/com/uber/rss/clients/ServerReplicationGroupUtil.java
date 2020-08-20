package com.uber.rss.clients;

import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ServerReplicationGroupUtil {
  private static final Logger logger = LoggerFactory.getLogger(ServerReplicationGroupUtil.class);

  /**
   * Create a list of replication groups from given servers
   * @param servers
   * @param numReplicas
   * @return
   */
  public static List<ServerReplicationGroup> createReplicationGroups(Collection<ServerDetail> servers, int numReplicas) {
    if (servers.isEmpty()) {
      throw new IllegalArgumentException("Invalid argument: servers is empty");
    }

    if (numReplicas <= 0) {
      throw new IllegalArgumentException(String.format("Invalid argument: numReplicas: %s", numReplicas));
    }

    if (servers.size() < numReplicas) {
      throw new RssInvalidStateException(String.format("Lack of enough servers (%s) to support %s replicas", servers.size(), numReplicas));
    }

    List<ServerDetail> serverList = new ArrayList<>(servers);

    List<ServerReplicationGroup> result = new ArrayList<>(servers.size());
    for (int i = 0; i < servers.size(); i+=numReplicas) {
      // do not use remaining servers if they are not enough for the replicas
      if (servers.size() - i < numReplicas) {
        continue;
      }

      List<ServerDetail> serverGroup = new ArrayList<>();
      for (int j = 0; j < numReplicas; j++) {
        serverGroup.add(serverList.get((i + j) % servers.size()));
      }
      result.add(new ServerReplicationGroup(serverGroup));
    }
    return result;
  }

  /***
   * Create replication groups for a given partition
   * @param servers
   * @param numReplicas
   * @param partition
   * @param partitionFanout
   * @return
   */
  public static List<ServerReplicationGroup> createReplicationGroupsForPartition(Collection<ServerDetail> servers, int numReplicas, int partition, int partitionFanout) {
    List<ServerReplicationGroup> result = new ArrayList<>();
    List<ServerReplicationGroup> serverReplicationGroups = createReplicationGroups(servers, numReplicas);
    if (partitionFanout > serverReplicationGroups.size()) {
      throw new RssInvalidStateException(String.format(
          "Cannot get server replication groups for partition %s, number of servers: %s, replicas: %s, partition fanout: %s",
          partition,
          servers.size(),
          numReplicas,
          partitionFanout));
    }
    int startIndex = partition % serverReplicationGroups.size();
    for (int i = 0; i < partitionFanout; i++) {
      int index = (startIndex + i) % serverReplicationGroups.size();
      result.add(serverReplicationGroups.get(index));
    }

    return result;
  }
}
