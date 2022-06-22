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

package org.apache.spark.shuffle.internal

import com.esotericsoftware.kryo.io.Input
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoSerializer, KryoSerializerInstance}
import org.testng.Assert
import org.testng.annotations.Test

import java.util.Random

class KryoWriteBufferManagerTest {
  val serializer = new KryoSerializer(getConf)
  val maxBufferSize = 10000000

  @Test
  def singlePartition(): Unit = {
    var bufferSize = 2
    val spillSize = 100
    val numPartitions = 10
    val record = (1, "123") // it is 7 bytes after serialization
    var bufferManager = new KyroRecordBufferManager[Any, Any](
      serializer.newInstance().asInstanceOf[KryoSerializerInstance],
      bufferSize, maxBufferSize, spillSize, numPartitions)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    var spilledData = bufferManager.clear().toList
    Assert.assertEquals(spilledData.size, 0)

    val partition1 = 1
    spilledData = bufferManager.addRecord(partition1, record).toList
    spilledData ++= bufferManager.clear()
    Assert.assertEquals(spilledData.size, 1)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    spilledData = bufferManager.clear().toList
    Assert.assertEquals(spilledData.size, 0)

    bufferSize = 20
    bufferManager = new KyroRecordBufferManager[Any, Any](
      serializer.newInstance().asInstanceOf[KryoSerializerInstance],
      bufferSize, maxBufferSize, spillSize, numPartitions)
    spilledData = bufferManager.clear().toList
    Assert.assertEquals(spilledData.size, 0)

    spilledData = bufferManager.addRecord(partition1, record).toList
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertTrue(bufferManager.filledBytes > 0)

    spilledData = bufferManager.addRecord(partition1, record).toList
    Assert.assertEquals(spilledData.size, 0)

    spilledData = bufferManager.addRecord(partition1, record).toList
    spilledData ++= bufferManager.clear()
    Assert.assertEquals(spilledData.size, 1)

    Assert.assertEquals(bufferManager.filledBytes, 0)

    spilledData = bufferManager.addRecord(partition1, record).toList
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertTrue(bufferManager.filledBytes > 0)

    spilledData = bufferManager.addRecord(partition1, record).toList
    Assert.assertEquals(spilledData.size, 0)

    spilledData = bufferManager.addRecord(partition1, record).toList
    spilledData ++= bufferManager.clear()
    Assert.assertEquals(spilledData.size, 1)

    Assert.assertEquals(bufferManager.filledBytes, 0)

    spilledData = bufferManager.clear().toList
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertEquals(bufferManager.filledBytes, 0)
  }

  @Test
  def multiPartitions(): Unit = {
    val bufferSize = 20
    val spillSize = 30
    val numPartitions = 10
    val bufferManager = new KyroRecordBufferManager[Any, Any](
      serializer.newInstance().asInstanceOf[KryoSerializerInstance],
      bufferSize, maxBufferSize, spillSize, numPartitions)
    var spilledData = bufferManager.clear().toList
    Assert.assertEquals(spilledData.size, 0)

    val partition1 = 1
    val partition2 = 2
    val record1 = (1, "123") // it is 7 bytes after serialization
    val record2 = (1, "124")
    val record3 = (1, "125")
    spilledData = bufferManager.addRecord(partition1, record1).toList
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertTrue(bufferManager.filledBytes > 0)

    spilledData = bufferManager.addRecord(partition2, record2).toList
    Assert.assertEquals(spilledData.size, 0)

    spilledData = bufferManager.addRecord(partition1, record2).toList
    Assert.assertEquals(spilledData.size, 0)

    spilledData = bufferManager.addRecord(partition1, record3).toList
    spilledData ++= bufferManager.clear().toList
    spilledData = spilledData.sortBy(_._1)
    Assert.assertEquals(spilledData.size, 2)

    Assert.assertEquals(spilledData(0)._1, partition1)
    var deserializedData = deserializeData(spilledData(0)._2, spilledData(0)._3).toList
    Assert.assertEquals(deserializedData, List(record1, record2, record3))

    Assert.assertEquals(spilledData(1)._1, partition2)
    deserializedData = deserializeData(spilledData(1)._2, spilledData(1)._3).toList
    Assert.assertEquals(deserializedData, List(record2))

    Assert.assertEquals(bufferManager.filledBytes, 0)
  }

  @Test
  def totalSizeExceedSpillSize(): Unit = {
    val bufferSize = 10000
    val spillSize = 20
    val numPartitions = 10
    val bufferManager = new KyroRecordBufferManager[Any, Any](
      serializer.newInstance().asInstanceOf[KryoSerializerInstance],
      bufferSize, maxBufferSize, spillSize, numPartitions)

    val partition1 = 1
    val partition2 = 2
    val partition3 = 3
    val record = (1, "123") // it is 7 bytes after serialization
    var spilledData = bufferManager.addRecord(partition1, record).toList
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertTrue(bufferManager.filledBytes > 0)

    spilledData = bufferManager.addRecord(partition2, record).toList
    Assert.assertEquals(spilledData.size, 0)

    spilledData = bufferManager.addRecord(partition3, record).toList

    Assert.assertEquals(spilledData.size, 3)
    Assert.assertEquals(spilledData.map(_._1).sorted, Seq(partition1, partition2, partition3))
    Assert.assertEquals(bufferManager.filledBytes, 0)

    (0 until 1000000).foreach(_ => {
      bufferManager.addRecord(partition3, record)
    })
    Assert.assertNotEquals(bufferManager.filledBytes, 0)
  }

  @Test
  def randomTest(): Unit = {
    val bufferSize = 10
    val spillSize = 20
    val numPartitions = 10
    val partitions = List(1, 2, 3, 4, 5)
    val records = List((1, "123"), (1, 2), (1, "123456789"), ("123456789", "123456789"))
    val recordSet = records.toSet

    val bufferManager = new KyroRecordBufferManager[Any, Any](
      serializer.newInstance().asInstanceOf[KryoSerializerInstance],
      bufferSize, maxBufferSize, spillSize, numPartitions)

    val numRecords = 2000
    val random = new Random()
    var numDeserializedRecords = 0
    (0 until numRecords).foreach(_ => {
      val partition = partitions(random.nextInt(partitions.size))
      val record = records(random.nextInt(records.size))
      val spilledData = bufferManager.addRecord(partition, record)

      Assert.assertTrue(bufferManager.filledBytes >= 0)

      val deserializedRecords = spilledData.flatMap(t => deserializeData(t._2, t._3)).toList
      numDeserializedRecords += deserializedRecords.size

      deserializedRecords.foreach(t => {
        Assert.assertTrue(recordSet.contains(t))
      })
    })

    val remainingData = bufferManager.clear()
    Assert.assertEquals(bufferManager.filledBytes, 0)

    numDeserializedRecords += remainingData.flatMap(t => deserializeData(t._2, t._3)).size

    Assert.assertEquals(numDeserializedRecords, numRecords)
  }

  private def getConf = {
    new SparkConf().setAppName("testApp")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  private def deserializeData(data: Array[Byte], length: Int): Seq[(Any, Any)] = {
    val input = new Input(data, 0, length)
    val stream = serializer.newInstance().deserializeStream(input)
    stream.asKeyValueIterator.toList
  }
}
