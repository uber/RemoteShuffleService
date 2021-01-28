package org.apache.spark.shuffle.rss

import com.esotericsoftware.kryo.io.Output
import org.apache.spark.internal.Logging
import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.RssOpts
import org.apache.spark.util.collection.PartitionedAppendOnlyMap

import scala.collection.mutable

private[rss] class WriterAggregationImpl[K, V, C](taskMemoryManager: TaskMemoryManager,
                                     shuffleDependency: ShuffleDependency[K, V, C],
                                     serializer: Serializer,
                                     bufferOptions: BufferManagerOptions,
                                     conf: SparkConf)
  extends MemoryConsumer(taskMemoryManager) with Logging {

  private var map = new PartitionedAppendOnlyMap[K, C]
  private var recordsWrittenCnt: Int = 0
  private var forceSpill: Boolean = false
  private var numberOfSpills: Int = 0

  private val mergeValue = shuffleDependency.aggregator.get.mergeValue
  private val createCombiner = shuffleDependency.aggregator.get.createCombiner
  private var kv: Product2[K, V] = null
  private val update = (hadValue: Boolean, oldValue: C) => {
    if (hadValue) {
      mergeValue(oldValue, kv._2)
    } else {
      createCombiner(kv._2)
    }
  }

  private val initialMemoryThreshold: Long = conf.get(RssOpts.rssMapSideAggInitialMemoryThreshold)
  private val allocateMemoryDynamically: Boolean = conf.get(RssOpts.enableDynamicMemoryAllocation)
  private val serializerInstance = serializer.newInstance()
  private var allocatedMemoryThreshold: Long = initialMemoryThreshold

  private[rss] def mapSize: Int = map.size
  private[rss] def recordsWritten: Int = recordsWrittenCnt

  private def changeValue(key: (Int, K), updateFunc: (Boolean, C) => C): C = {
    map.changeValue(key, updateFunc)
  }

  private[rss] def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    kv = record
    changeValue((partitionId, kv._1), update)
    maybeSpillCollection()
  }

  private def maybeSpillCollection(): Seq[(Int, Array[Byte])] = {
    if (forceSpill) {
      val dataToSpill = spillMap()
      forceSpill = false
      dataToSpill
    } else {
      val estimatedSize = map.estimateSize()
      val (spill, spilledData) = mayBeSpill(estimatedSize)
      if (spill) {
        spilledData
      } else {
        Seq.empty
      }
    }
  }

  private def mayBeSpill(currentMemory: Long): (Boolean, Seq[(Int, Array[Byte])])  = {
    if (currentMemory >= allocatedMemoryThreshold) {
      if (!tryToAllocateMemory(currentMemory)) {
        logDebug(s"Exhausted allocated $allocatedMemoryThreshold memory. Spilling $currentMemory to RSS servers")
        val result = spillMap()
        (true, result)
      } else {
        (false, Seq.empty)
      }
    } else {
      (false, Seq.empty)
    }
  }

  def releaseMemory(memoryToHold: Long): Long = {
    if (allocateMemoryDynamically) {
      val memoryReleased = allocatedMemoryThreshold - memoryToHold
      freeMemory(memoryReleased)
      allocatedMemoryThreshold = memoryToHold
      memoryReleased
    } else {
      0L
    }
  }

  private def tryToAllocateMemory(currentMemory: Long): Boolean = {
    if (allocateMemoryDynamically) {
      val memoryToRequest = 2 * currentMemory - allocatedMemoryThreshold
      val granted = acquireMemory(memoryToRequest)
      allocatedMemoryThreshold += granted
      allocatedMemoryThreshold >= currentMemory
    } else {
      false
    }
  }

  private[rss] def spillMap(): Seq[(Int, Array[Byte])] = {
    numberOfSpills += 1
    val result = mutable.Buffer[(Int, Array[Byte])]()
    val output = new Output(initialMemoryThreshold.toInt, bufferOptions.individualBufferMax)
    val stream = serializerInstance.serializeStream(output)

    val itr = map.iterator
    //ToDo: Skip map side aggregation based on the reduction factor
    while (itr.hasNext) {
      val item = itr.next()
      val (key, value): Product2[Any, Any] = (item._1._2, item._2)
      stream.writeKey(key)
      stream.writeValue(value)
      stream.flush()
      result.append((item._1._1, output.toBytes))
      output.clear()
      recordsWrittenCnt += 1
    }
    stream.close()
    map = new PartitionedAppendOnlyMap[K, C]
    result
  }

  private[rss] def filledBytes: Int = {
    if (map.isEmpty) {
      0
    } else {
      map.estimateSize().toInt
    }
  }

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (allocateMemoryDynamically && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
      val dataToSpill = spillMap()
      if (!dataToSpill.isEmpty) {
        // set forceSpill so that the data will be force spilled whenever new record is to be written
        forceSpill = true
        // ToDo: This memory will be released when the next record is processed and after sendDataBlock
        //  has been called. Will this cause issues?
        val memoryToBeReleased = allocatedMemoryThreshold - initialMemoryThreshold
        memoryToBeReleased
      } else {
        0L
      }
    } else {
      0L
    }
  }}
