package org.apache.spark.shuffle

import com.uber.rss.common.ServerList
import org.apache.spark.ShuffleDependency

private[spark] class RssShuffleHandle[K, V, C](
    shuffleId: Int,
    val appId: String,
    val appAttempt: String,
    val numMaps: Int,
    val user: String,
    val queue: String,
    val dependency: ShuffleDependency[K, V, C],
    val rssServers: Array[RssShuffleServerHandle],
    val partitionFanout: Int = 1)
  extends ShuffleHandle(shuffleId) {

  def getServerList: ServerList = {
    new ServerList(rssServers.map(_.toServerDetail()))
  }

  override def toString: String = s"RssShuffleHandle (shuffleId $shuffleId, numMaps: $numMaps, rssServers: ${rssServers.length} servers), partitionFanout: $partitionFanout"
}