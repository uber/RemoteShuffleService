package org.apache.spark.shuffle

import com.uber.rss.common.AppShuffleId
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.rss.BlockDownloaderPartitionRangeRecordIterator
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, ShuffleDependency, TaskContext}

/***
 * This is a shuffle reader returning zero record.
 * It is used when there is zero partitions for mapper side. So the reader could return
 * empty record iterator directly without connecting to shuffle server.
 * @tparam K
 * @tparam C
 */
class RssEmptyShuffleReader[K, C](
    shuffleInfo: AppShuffleId,
    startPartition: Int,
    endPartition: Int) extends ShuffleReader[K, C] with Logging {

  logInfo(s"Using RssEmptyShuffleReader: ${this.getClass.getSimpleName}, shuffleInfo: $shuffleInfo, startPartition: $startPartition, endPartition: $endPartition")
  
  override def read(): Iterator[Product2[K, C]] = {
    logInfo(s"RssEmptyShuffleReader read")
    Iterator.empty
  }
}
