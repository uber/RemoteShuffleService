package org.apache.spark.shuffle

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkConf
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.storage.BlockId

class RssTestMemoryManager(conf: SparkConf)
  extends MemoryManager(conf, numCores = 1, Long.MaxValue, Long.MaxValue) {

  override def acquireExecutionMemory(
                                       numBytes: Long,
                                       taskAttemptId: Long,
                                       memoryMode: MemoryMode): Long = {
    if (consequentOOM > 0) {
      consequentOOM -= 1
      0
    } else if (available >= numBytes) {
      available -= numBytes
      numBytes
    } else {
      val grant = available
      available = 0
      grant
    }
  }
  override def acquireStorageMemory(
                                     blockId: BlockId,
                                     numBytes: Long,
                                     memoryMode: MemoryMode): Boolean = true
  override def acquireUnrollMemory(
                                    blockId: BlockId,
                                    numBytes: Long,
                                    memoryMode: MemoryMode): Boolean = true
  override def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = {}
  override def releaseExecutionMemory(
                                       numBytes: Long,
                                       taskAttemptId: Long,
                                       memoryMode: MemoryMode): Unit = {
    available += numBytes
  }
  override def maxOnHeapStorageMemory: Long = Long.MaxValue

  override def maxOffHeapStorageMemory: Long = 0L

  private var consequentOOM = 0
  private var available = Long.MaxValue

  def markExecutionAsOutOfMemoryOnce(): Unit = {
    markconsequentOOM(1)
  }

  def markconsequentOOM(n : Int) : Unit = {
    consequentOOM += n
  }

  def limit(avail: Long): Unit = {
    available = avail
  }

}