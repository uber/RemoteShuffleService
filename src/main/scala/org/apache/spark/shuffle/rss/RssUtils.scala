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

import java.util
import java.util.Collection
import java.util.function.Supplier

import com.fasterxml.jackson.databind.ObjectMapper
import com.uber.rss.clients._
import com.uber.rss.clients.PlainRecordSocketReadClient
import com.uber.rss.common.{AppShufflePartitionId, ServerDetail, ServerList, ServerReplicationGroup}
import com.uber.rss.exceptions.RssException
import com.uber.rss.util.RetryUtils
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.RssShuffleServerHandle
import org.apache.spark.storage.BlockManagerId

import scala.collection.{JavaConverters, immutable}

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
                                       stageAttemptNumber: Int,
                                       rssServers: ServerList = new ServerList(new util.ArrayList[ServerDetail]())): BlockManagerId = {
    // Spark will check the host and port in BlockManagerId, thus use dummy values there
    val dummyHost = "dummy_host"
    val dummyPort = 99999
    // hack: use execId field in BlockManagerId to store map id and task attempt id
    val topologyInfo = if (rssServers.getSevers.isEmpty) {
      ""
    } else {
      val mapper = new ObjectMapper()
      mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
      mapper.writeValueAsString(new MapAttemptRssInfo(mapId, taskAttemptId, stageAttemptNumber, rssServers))
    }
    BlockManagerId(s"map_$mapId" + s"_$taskAttemptId", dummyHost, dummyPort, Some(topologyInfo))
  }

  /***
   * Get rss servers from dummy BlockManagerId
   * @param blockManagerId BlockManagerId instance
   * @return
   */
  def getRssServersFromBlockManagerId(blockManagerId: BlockManagerId): Option[MapAttemptRssInfo] = {
    val topologyInfo = blockManagerId.topologyInfo.getOrElse("")
    if (topologyInfo.isEmpty) {
      return None
    }

    val mapper = new ObjectMapper()
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    val blockManagerTopologyInfo = mapper.readValue(topologyInfo, classOf[MapAttemptRssInfo])
    Some(blockManagerTopologyInfo)
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
    val mapLatestAttemptRssServers = scala.collection.mutable.HashMap[Int, MapAttemptRssInfo]()
    val mapAttemptRssInfoList =
      RetryUtils.retry(retryIntervalMillis,
        retryIntervalMillis * 10,
        maxRetryMillis,
        s"get information from map output tracker, shuffleId: $shuffleId, partition: $partition",
        new Supplier[Seq[MapAttemptRssInfo]] {
          override def get(): Seq[MapAttemptRssInfo] = {
            val mapStatusInfo = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId, partition, partition + 1)
            mapStatusInfo.flatMap(mapStatusInfoEntry=>RssUtils.getRssServersFromBlockManagerId(mapStatusInfoEntry._1)).toList
          }
        })

    if (mapAttemptRssInfoList.isEmpty) {
      throw new RssException(s"Failed to get information from map output tracker, shuffleId: $shuffleId, partition: $partition")
    }
    val stageAttemptNumbers = mapAttemptRssInfoList.map(_.stageAttemptNumber).distinct.toList
    val stageAttemptNumbersStr = stageAttemptNumbers.mkString(",")
    val stageAttemptNumber = stageAttemptNumbers.max
    logInfo(s"Found ${stageAttemptNumbers.size} stage attempts for shuffle $shuffleId: $stageAttemptNumbersStr, use $stageAttemptNumber as latest stage attempt number")
    for (mapAttemptRssInfo <- mapAttemptRssInfoList) {
      if (mapAttemptRssInfo.stageAttemptNumber == stageAttemptNumber) {
        val mapId = mapAttemptRssInfo.mapId
        val oldValue = mapLatestAttemptRssServers.get(mapId)
        if (oldValue.isEmpty || oldValue.get.taskAttemptId < mapAttemptRssInfo.taskAttemptId) {
          mapLatestAttemptRssServers.put(mapId, mapAttemptRssInfo)
        }
      }
    }
    val numMaps = mapLatestAttemptRssServers.size
    val serverLists = mapLatestAttemptRssServers.values
      .map(_.rssServers)
      .toArray
      .distinct
    val latestTaskAttemptIds = mapLatestAttemptRssServers.values
      .map(_.taskAttemptId)
      .toArray
      .distinct
    new MapOutputRssInfo(numMaps, serverLists, stageAttemptNumber, latestTaskAttemptIds)
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
}
