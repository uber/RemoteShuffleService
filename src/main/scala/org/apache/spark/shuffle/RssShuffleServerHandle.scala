package org.apache.spark.shuffle

import com.uber.rss.common.ServerDetail

case class RssShuffleServerHandle(serverId: String, runningVersion: String, connectionString: String){
  def toServerDetail(): ServerDetail = {
    new ServerDetail(serverId, runningVersion, connectionString)
  }
}
