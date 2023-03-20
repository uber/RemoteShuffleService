/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.rss

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManagerId

object RssUtils extends Logging {

  def getRssServerReplicationGroups(rssServers: ServerList, numReplicas: Int, partitionId: Int, partitionFanout: Int): java.util.List[ServerReplicationGroup] = {
    ServerReplicationGroupUtil.createReplicationGroupsForPartition(rssServers.getSevers, numReplicas, partitionId, partitionFanout)
  }

  /**
   * Create dummy BlockManagerId and embed shuffle servers inside it.
   * @param mapId map id
   * @param taskAttemptId task attempt id
   * @param stageAttemptNumber stage attempt number
   * @param rssServers rss servers
   * @return
   */
  def createMapTaskDummyBlockManagerId(mapId: Int,
                                       taskAttemptId: Long,
                                       rssServers: ServerList = new ServerList(new util.ArrayList[ServerDetail]())): BlockManagerId = {
    // Spark will check the host and port in BlockManagerId, thus use dummy values there
    val dummyHost = "dummy_host"
    val dummyPort = 99999
    // hack: use execId field in BlockManagerId to store map id and task attempt id
    val serverList = rssServers.getSevers
    val topologyInfo = if (serverList.isEmpty) {
      ""
    } else {
      val rssInfo = new MapTaskRssInfo(mapId, taskAttemptId, serverList.size())
      rssInfo.serializeToString()
    }
    BlockManagerId(s"map_$mapId" + s"_$taskAttemptId", dummyHost, dummyPort, Some(topologyInfo))
  }

  /***
   * Get rss information from dummy BlockManagerId
   * @param blockManagerId BlockManagerId instance
   * @return
   */
  def getRssInfoFromBlockManagerId(blockManagerId: BlockManagerId): Option[MapTaskRssInfo] = {
    val topologyInfo = blockManagerId.topologyInfo.getOrElse("")
    if (topologyInfo.isEmpty) {
      return None
    }

    val rssInfo = MapTaskRssInfo.deserializeFromString(topologyInfo)
    Some(rssInfo)
  }

  /***
   * Get rss information from map output tracker. Each map task should send rss servers to map output tracker
   * when the map task finishes, so we could query map output tracker to get the servers. Because rss server
   * may restart among different map tasks, different map tasks may send different rss servers to map output
   * tracker. This method will get all these servers and return an array of server lists.
   * @param shuffleId shuffle id
   * @param partition partition id
   * @return
   */
  def getRssInfoFromMapOutputTracker(shuffleId: Int, partition: Int, retryIntervalMillis: Long, maxRetryMillis: Long): MapOutputRssInfo = {
    // this hash map stores rss servers for each map task's latest attempt
    val mapLatestAttemptRssServers = scala.collection.mutable.HashMap[Int, MapTaskRssInfo]()
    val mapAttemptRssInfoList =
      RetryUtils.retry(retryIntervalMillis,
        retryIntervalMillis * 10,
        maxRetryMillis,
        s"get information from map output tracker, shuffleId: $shuffleId, partition: $partition",
        new Supplier[Seq[MapTaskRssInfo]] {
          override def get(): Seq[MapTaskRssInfo] = {
            val mapStatusInfo = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId, partition, partition + 1)
            logInfo(s"Got result from mapOutputTracker.getMapSizesByExecutorId")
            mapStatusInfo.toParArray.flatMap(mapStatusInfoEntry=>RssUtils.getRssInfoFromBlockManagerId(mapStatusInfoEntry._1)).toList
          }
        })
    logInfo(s"Got ${mapAttemptRssInfoList.size} items after parsing mapOutputTracker.getMapSizesByExecutorId result")
    if (mapAttemptRssInfoList.isEmpty) {
      throw new RssInvalidMapStatusException(s"Failed to get information from map output tracker, shuffleId: $shuffleId, partition: $partition")
    }
    for (mapAttemptRssInfo <- mapAttemptRssInfoList) {
      val mapId = mapAttemptRssInfo.getMapId
      val oldValue = mapLatestAttemptRssServers.get(mapId)
      if (oldValue.isEmpty || oldValue.get.getTaskAttemptId < mapAttemptRssInfo.getTaskAttemptId) {
        mapLatestAttemptRssServers.put(mapId, mapAttemptRssInfo)
      }
    }
    val numMaps = mapLatestAttemptRssServers.size
    val numRssServersValues = mapLatestAttemptRssServers.values
      .map(_.getNumRssServers)
      .toList
      .distinct
    if (numRssServersValues.size != 1) {
      throw new RssInvalidMapStatusException(s"Got invalid number of RSS servers: $numRssServersValues")
    }
    val numRssServers = numRssServersValues.head
    val mapTaskAttemptIds = mapLatestAttemptRssServers.values
      .map(_.getTaskAttemptId)
      .toArray
      .distinct
    MapOutputRssInfo(numMaps, numRssServers, mapTaskAttemptIds)
  }

  /**
   * Create dummy BlockManagerId for reduce task.
   * @param shuffleId shuffle id
   * @param partition partition
   * @return
   */
  def createReduceTaskDummyBlockManagerId(shuffleId: Int, partition: Int): BlockManagerId = {
    // Spark will check the host and port in BlockManagerId, thus use dummy values there
    val dummyHost = "dummy_host"
    val dummyPort = 99999
    BlockManagerId(s"reduce_${shuffleId}_$partition", dummyHost, dummyPort, None)
  }

  def isUsingRSSForShuffle(shuffleHandle: ShuffleHandle, conf: SparkConf): Boolean = {
    shuffleHandle.getClass.getName.equals("org.apache.spark.shuffle.RssShuffleHandle")
  }
}
