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

package org.apache.spark.shuffle

import com.uber.rss.common.{AppShuffleId, ServerList}
import com.uber.rss.metadata.ServiceRegistry
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.rss.BlockDownloaderPartitionRangeRecordIterator
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, ShuffleDependency, TaskContext}

class RssShuffleReader[K, C](
                              user: String,
                              shuffleInfo: AppShuffleId,
                              startPartition: Int,
                              endPartition: Int,
                              serializer: Serializer,
                              context: TaskContext,
                              shuffleDependency: ShuffleDependency[K, _, C],
                              numMaps: Int,
                              rssServers: ServerList,
                              partitionFanout: Int,
                              serviceRegistry: ServiceRegistry,
                              serviceRegistryDataCenter: String,
                              serviceRegistryCluster: String,
                              timeoutMillis: Int,
                              maxRetryMillis: Int,
                              dataAvailablePollInterval: Long,
                              dataAvailableWaitTime: Long,
                              queueSize: Int,
                              shuffleReplicas: Int,
                              checkShuffleReplicaConsistency: Boolean) extends ShuffleReader[K, C] with Logging {

  logInfo(s"Using ShuffleReader: ${this.getClass.getSimpleName}, queueSize: $queueSize")

  override def read(): Iterator[Product2[K, C]] = {
    logInfo(s"Shuffle read started: $shuffleInfo, partitions: [$startPartition, $endPartition)")

    val partitionRecordIterator = new BlockDownloaderPartitionRangeRecordIterator(
      user = user,
      appId = shuffleInfo.getAppId,
      appAttempt = shuffleInfo.getAppAttempt,
      shuffleId = shuffleInfo.getShuffleId,
      startPartition = startPartition,
      endPartition = endPartition,
      serializer = serializer,
      numMaps = numMaps,
      rssServers = rssServers,
      partitionFanout = partitionFanout,
      serviceRegistry = serviceRegistry,
      serviceRegistryDataCenter = serviceRegistryDataCenter,
      serviceRegistryCluster = serviceRegistryCluster,
      timeoutMillis = timeoutMillis,
      maxRetryMillis = maxRetryMillis,
      dataAvailablePollInterval = dataAvailablePollInterval,
      dataAvailableWaitTime = dataAvailableWaitTime,
      queueSize = queueSize,
      shuffleReplicas = shuffleReplicas,
      checkShuffleReplicaConsistency = checkShuffleReplicaConsistency,
      shuffleReadMetrics = context.taskMetrics().shuffleReadMetrics
    )

    val dep = shuffleDependency
    
    logInfo(s"dep.aggregator.isDefined: ${dep.aggregator.isDefined}, dep.mapSideCombine: ${dep.mapSideCombine}, dep.keyOrdering: ${dep.keyOrdering}")
    
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        dep.aggregator.get.combineCombinersByKey(partitionRecordIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = partitionRecordIterator.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      partitionRecordIterator
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data
        val sorter = new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        logInfo(s"Inserting aggregated records to sorter: $shuffleInfo")
        val startTime = System.currentTimeMillis()
        sorter.insertAll(aggregatedIter)
        logInfo(s"Inserted aggregated records to sorter: $shuffleInfo, partition [$startPartition, $endPartition), millis: ${System.currentTimeMillis() - startTime}")
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener(_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}
