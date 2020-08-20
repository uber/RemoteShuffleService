package org.apache.spark.shuffle.rss

import scala.collection.JavaConversions

import com.uber.rss.common.ServerList

/**
 * This class stores RSS information for a map task attempt.
 * @param mapId
 * @param taskAttemptId
 * @param stageAttemptNumber
 * @param rssServers
 */
case class MapAttemptRssInfo(mapId: Int, taskAttemptId: Long, stageAttemptNumber: Int, rssServers: ServerList) {
  override def toString: String = {
    val rssServersStr = JavaConversions.asScalaBuffer(rssServers.getSevers).mkString(",")
    s"MapAttemptRssServers(mapId: $mapId, taskAttemptId: $taskAttemptId, stageAttemptNumber: $stageAttemptNumber, rssServers: $rssServersStr)"
  }
}
