package org.apache.spark.shuffle

import com.uber.rss.common.ServerDetail

case class RssServerSelectionResult(servers: Array[ServerDetail], replicas: Int, partitionFanout: Int)
