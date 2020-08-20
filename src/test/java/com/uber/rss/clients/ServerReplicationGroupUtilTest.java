package com.uber.rss.clients;

import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssInvalidStateException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ServerReplicationGroupUtilTest {

  @Test
  public void createReplicationGroups_oneReplica() {
    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroups(Arrays.asList(new ServerDetail("server1", "001", "host:9000")), 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    Assert.assertEquals(replicationGroups.get(0).getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));

    replicationGroups = ServerReplicationGroupUtil.createReplicationGroups(Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")),
        1);
    Assert.assertEquals(replicationGroups.size(), 2);
    Assert.assertEquals(replicationGroups.get(0).getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));
    Assert.assertEquals(replicationGroups.get(1).getServers(), Arrays.asList(new ServerDetail("server2", "001", "host2:9000")));
  }

  @Test(expectedExceptions = RssInvalidStateException.class)
  public void createReplicationGroups_twoReplicas_oneServer() {
    ServerReplicationGroupUtil.createReplicationGroups(Arrays.asList(new ServerDetail("server1", "001", "host:9000")), 2);
  }

  @Test
  public void createReplicationGroups_twoReplicas_twoServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"));

    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroups(serverDetailList, 2);
    Assert.assertEquals(replicationGroups.size(), 1);
    Assert.assertEquals(replicationGroups.get(0).getServers(), new ArrayList<>(serverDetailList));
  }

  @Test
  public void createReplicationGroups_twoReplicas_fourServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000"));

    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroups(serverDetailList, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    Assert.assertEquals(replicationGroups.get(0).getServers(),
        Arrays.asList(new ServerDetail("server1", "001", "host:9000"), new ServerDetail("server2", "001", "host2:9000")));
    Assert.assertEquals(replicationGroups.get(1).getServers(),
        Arrays.asList(new ServerDetail("server3", "001", "host3:9000"), new ServerDetail("server4", "001", "host4:9000")));
  }

  @Test
  public void createReplicationGroups_twoReplicas_fiveServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000"),
        new ServerDetail("server5", "001", "host5:9000"));

    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroups(serverDetailList, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    Assert.assertEquals(replicationGroups.get(0).getServers(),
        Arrays.asList(new ServerDetail("server1", "001", "host:9000"), new ServerDetail("server2", "001", "host2:9000")));
    Assert.assertEquals(replicationGroups.get(1).getServers(),
        Arrays.asList(new ServerDetail("server3", "001", "host3:9000"), new ServerDetail("server4", "001", "host4:9000")));
  }

  @Test
  public void createReplicationGroups_threeReplicas_fiveServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000"),
        new ServerDetail("server5", "001", "host5:9000"));

    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroups(serverDetailList, 3);
    Assert.assertEquals(replicationGroups.size(), 1);
    Assert.assertEquals(replicationGroups.get(0).getServers(),
        Arrays.asList(
            new ServerDetail("server1", "001", "host:9000"),
            new ServerDetail("server2", "001", "host2:9000"),
            new ServerDetail("server3", "001", "host3:9000")));
  }

  @Test(expectedExceptions = RssInvalidStateException.class)
  public void createReplicationGroupsForPartition_twoServersPerPartition_oneServer() {
    ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(new ServerDetail("server1", "001", "host:9000")), 1, 0, 2);
  }

  @Test
  public void createReplicationGroupsForPartition_oneReplica() {
    int partition = 0;
    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(new ServerDetail("server1", "001", "host:9000")), 1, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    ServerReplicationGroup replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(new ServerDetail("server1", "001", "host:9000")), 1, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));

    partition = 0;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")),
        1,
        partition,
        1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));

    // two servers per partition
    partition = 0;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")),
        1,
        partition,
        2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server2", "001", "host2:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")),
        1,
        partition,
        1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server2", "001", "host2:9000")));

    // two servers per partition
    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")),
        1,
        partition,
        2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server2", "001", "host2:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));

    partition = 2;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")),
        1,
        partition,
        1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));

    // two servers per partition
    partition = 2;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")),
        1,
        partition,
        2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server1", "001", "host:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(new ServerDetail("server2", "001", "host2:9000")));
  }

  @Test(expectedExceptions = RssInvalidStateException.class)
  public void createReplicationGroup_twoReplicas_oneServer() {
    int partition = 0;
    ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(new ServerDetail("server1", "001", "host:9000")), 2, partition, 1);
  }

  @Test(expectedExceptions = RssInvalidStateException.class)
  public void createReplicationGroupsForPartition_twoReplicas_twoServers_twoServersPerPartition() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"));

    int partition = 0;
    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
  }

  @Test
  public void createReplicationGroupsForPartition_twoReplicas_twoServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"));

    int partition = 0;
    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    ServerReplicationGroup replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));

    partition = 2;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));
  }

  @Test
  public void createReplicationGroupsForPartition_twoReplicas_fiveServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000"),
        new ServerDetail("server5", "001", "host5:9000"));

    int partition = 0;
    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    ServerReplicationGroup replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));

    // two servers per partition
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000")));

    // two servers per partition
    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));

    partition = 2;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));

    // two servers per partition
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000")));

    partition = 3;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000")));

    // two servers per partition
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));

    partition = 4;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));

    // two servers per partition
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000")));
  }

  @Test
  public void createReplicationGroupsForPartition_threeReplicas_fiveServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000"),
        new ServerDetail("server4", "001", "host4:9000"),
        new ServerDetail("server5", "001", "host5:9000"));

    int partition = 0;
    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    ServerReplicationGroup replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000")));

    partition = 2;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000")));

    partition = 3;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000")));

    partition = 4;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "001", "host:9000"),
        new ServerDetail("server2", "001", "host2:9000"),
        new ServerDetail("server3", "001", "host3:9000")));
  }
}
