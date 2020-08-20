package org.apache.spark.shuffle.rss

import com.uber.rss.common.ServerList

/**
 * This class stores RSS information which is retrieved from map output tracker
 * @param numMaps
 * @param serverLists
 * @param latestTaskAttemptIds
 */
case class MapOutputRssInfo(numMaps: Int, serverLists: Array[ServerList], latestTaskAttemptIds: Array[Long]) {
  override def toString: String = {
    val serverListsStr = serverLists.mkString(",")
    val latestTaskAttemptIdsStr = latestTaskAttemptIds.mkString(",")
    s"MapAttemptRssServers(numMaps: $numMaps, serverLists: $serverListsStr, latestTaskAttemptIds: $latestTaskAttemptIdsStr)"
  }
}
