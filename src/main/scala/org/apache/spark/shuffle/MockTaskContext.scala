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

import java.util.Properties
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener}

class MockTaskContext(val mockStageId: Int, val mockPartitionId: Int, val mockTaskAttemptId: Long = 0) extends TaskContext {
  override def isCompleted(): Boolean = true

  override def isInterrupted(): Boolean = false

  override def isRunningLocally(): Boolean = true

  override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): TaskContext = {
    this
  }

  override def stageId(): Int = mockStageId

  override def stageAttemptNumber(): Int = 0

  override def partitionId(): Int = mockPartitionId

  override def attemptNumber(): Int = 0

  override def taskAttemptId(): Long = {
    mockTaskAttemptId
  }

  override def getLocalProperty(key: String): String = {
    ""
  }

  override def taskMetrics(): TaskMetrics = {
    new TaskMetrics()
  }

  override def getMetricsSources(sourceName: String): Seq[Source] = {
    Seq()
  }

  override private[spark] def killTaskIfInterrupted(): Unit = {}

  override private[spark] def getKillReason(): Option[String] = {
    None
  }

  val testMemoryManager = new RssTestMemoryManager(new SparkConf())

  override private[spark] def taskMemoryManager(): TaskMemoryManager = new TaskMemoryManager(testMemoryManager, 0)

  override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {}

  override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = {}

  override private[spark] def markInterrupted(reason: String): Unit = {}

  override private[spark] def markTaskFailed(error: Throwable): Unit = {}

  override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = {}

  override private[spark] def fetchFailed: Option[FetchFailedException] = {
    None
  }

  override private[spark] def getLocalProperties: Properties = {
    new Properties()
  }
}
