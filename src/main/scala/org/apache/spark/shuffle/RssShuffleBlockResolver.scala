package org.apache.spark.shuffle

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.ShuffleBlockId

class RssShuffleBlockResolver extends ShuffleBlockResolver {
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    throw new RuntimeException("RssShuffleBlockResolver.getBlockData not implemented")
  }

  override def stop(): Unit = {
  }
}
