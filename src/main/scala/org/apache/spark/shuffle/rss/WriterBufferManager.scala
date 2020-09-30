package org.apache.spark.shuffle.rss

import com.esotericsoftware.kryo.io.Output
import com.uber.rss.exceptions.RssInvalidDataException
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer, SerializerInstance}

import scala.collection.mutable
import scala.collection.mutable.Map

case class BufferManagerOptions(individualBufferSize: Int, individualBufferMax: Int, bufferSpillThreshold: Int)

case class WriterBufferManagerValue(serializeStream: SerializationStream, output: Output)

class WriteBufferManager(serializer: Serializer,
                               bufferSize: Int,
                               maxBufferSize: Int,
                               spillSize: Int) extends Logging {
  private val map: Map[Int, WriterBufferManagerValue] = Map()

  private var totalBytes = 0

  private val serializerInstance = serializer.newInstance()

  def addRecord(partitionId: Int, record: Product2[Any, Any]): Seq[(Int, Array[Byte])] = {
    val result = mutable.Buffer[(Int, Array[Byte])]()
    map.get(partitionId) match {
      case Some(v) =>
        val stream = v.serializeStream
        val oldSize = v.output.position()
        stream.writeKey(record._1)
        stream.writeValue(record._2)
        stream.flush()
        val newSize = v.output.position()
        if (newSize >= bufferSize) {
          result.append((partitionId, v.output.toBytes))
          v.serializeStream.close()
          map.remove(partitionId)
          totalBytes -= oldSize
        } else {
          totalBytes += (newSize - oldSize)
        }
      case None =>
        val output = new Output(bufferSize, maxBufferSize)
        val stream = serializerInstance.serializeStream(output)
        stream.writeKey(record._1)
        stream.writeValue(record._2)
        stream.flush()
        val newSize = output.position()
        if (newSize >= bufferSize) {
          result.append((partitionId, output.toBytes))
          stream.close()
        } else {
          map.put(partitionId, WriterBufferManagerValue(stream, output))
          totalBytes = totalBytes + newSize
        }
    }

    if (totalBytes >= spillSize) {
      result.appendAll(map.map(t=>(t._1, t._2.output.toBytes)))
      map.foreach(t => t._2.serializeStream.close())
      map.clear()
      totalBytes = 0
    }

    result
  }

  def filledBytes = {
    val sum = map.map(_._2.output.position()).sum
    if (sum != totalBytes) {
      throw new RssInvalidDataException(s"Inconsistent internal state, total bytes is $totalBytes, but should be $sum")
    }
    totalBytes
  }

  def clear(): Seq[(Int, Array[Byte])] = {
    val result = map.map(t=>(t._1, t._2.output.toBytes)).toSeq
    map.foreach(t => t._2.serializeStream.close())
    map.clear()
    totalBytes = 0
    result
  }
}
