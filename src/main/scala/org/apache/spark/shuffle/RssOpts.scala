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
package org.apache.spark.shuffle

import com.uber.rss.storage.ShuffleFileStorage
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

object RssOpts {
  val maxServerCount: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.maxServerCount")
      .doc("max remote shuffle servers used for this application.")
      .intConf
      .createWithDefault(100)
  val minServerCount: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.minServerCount")
      .doc("min remote shuffle servers used for this application.")
      .intConf
      .createWithDefault(1)
  val serverRatio: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.serverRatio")
      .doc("how many executors mapping to one shuffle server.")
      .intConf
      .createWithDefault(20)
  val writerQueueSize: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.queueSize")
      .doc("writer queue size for shuffle writer to store shuffle records and send them to shuffle server in background threads.")
      .intConf
      .createWithDefault(1000)
  val writerMaxThreads: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.maxThreads")
      .doc("max number of threads for shuffle writer to store shuffle records and send them to shuffle server in background threads.")
      .intConf
      .createWithDefault(2)
  val writerAsyncFinish: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.writer.asyncFinish")
      .doc("whether use async mode for writer to finish uploading data.")
      .booleanConf
      .createWithDefault(false)
  val networkTimeout: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.networkTimeout")
      .doc("network timeout (milliseconds) for shuffle client.")
      .longConf
      .createWithDefault(5*60*1000L)
  val useConnectionPool: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.useConnectionPool")
      .doc("use connection pool for shuffle client.")
      .booleanConf
      .createWithDefault(false)
  val networkRetries: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.networkRetries")
      .doc("max network retries when retrying is supported, e.g. connecting to zookeeper.")
      .intConf
      .createWithDefault(5)
  val maxWaitTime: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.maxWaitTime")
      .doc("maximum wait time (milliseconds) for shuffle client, e.g. retry connecting to busy remote shuffle server.")
      .longConf
      .createWithDefault(3*60*1000L)
  val pollInterval: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.pollInterval")
      .doc("poll interval (milliseconds) to query remote shuffle server for status update, e.g. whether a map task's data flushed.")
      .intConf
      .createWithDefault(200)
  val readerDataAvailableWaitTime: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.reader.dataAvailableWaitTime")
      .doc("max wait time in shuffle reader to wait data ready in the shuffle server.")
      .longConf
      .createWithDefault(6*60*1000L)
  val readerBufferSize: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.reader.bufferSize")
      .doc("buffer size to use for shuffle reader to read from shuffle server.")
      .intConf
      .createWithDefault(ShuffleFileStorage.DEFAULT_BUFFER_SIZE)
  val readerQueueSize: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.reader.queueSize")
      .doc("reader queue size to use for shuffle reader to read from shuffle server when using background threads.")
      .intConf
      .createWithDefault(0)
  val dataCenter: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.dataCenter")
      .doc("data center for RSS cluster. If not specified, will try to get value from the environment.")
      .stringConf
      .createWithDefault("")
  val cluster: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.cluster")
      .doc("RSS cluster name.")
      .stringConf
      .createWithDefault("default")
  val serviceRegistryType: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serviceRegistry.type")
      .doc("type of service registry to use: zookeeper, standalone.")
      .stringConf
      .createWithDefault("zookeeper")
  val serviceRegistryZKServers: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serviceRegistry.zookeeper.servers")
      .doc("ZooKeeper host:port addresses. Specify more than one as a comma-separated string.")
      .stringConf
      .createWithDefault("")
  val serviceRegistryServer: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serviceRegistry.server")
      .doc("Registry server host:port addresses.")
      .stringConf
      .createWithDefault("")
  val compressionBufferSize: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.compressionBufferSize")
      .doc("compression buffer size if using compressed clients.")
      .intConf
      .createWithDefault(64 * 1024)
  val fileCompressionCodec: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.fileCompressionCodec")
      .doc("shuffle file compression codec.")
      .stringConf
      .createWithDefault("")
  val minSplits: ConfigEntry[Int] =
  ConfigBuilder("spark.shuffle.rss.minSplits")
    .doc("min number of splits for each shuffle partition on each shuffle server.")
    .intConf
    .createWithDefault(1)
  val maxSplits: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.maxSplits")
      .doc("max number of splits for each shuffle partition on each shuffle server.")
      .intConf
      .createWithDefault(200)
  val mapsPerSplit: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.mapsPerSplit")
      .doc("how many map tasks write to same shuffle partition split. Large value here will " +
        "have more memory consumption because RSS client needs to maintain internal memory buffer" +
        "for each task.")
      .intConf
      .createWithDefault(500)
  val replicas: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.replicas")
      .doc("number of replicas for replicated shuffle client.")
      .intConf
      .createWithDefault(1)
  val checkReplicaConsistency: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.checkReplicaConsistency")
      .doc("Check replica data consistency when reading replicas. If set to true, it will consume more memory to track the data in client side.")
      .booleanConf
      .createWithDefault(false)
  val excludeHosts: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.excludeHosts")
      .doc("the server hosts to exclude, separated by comma.")
      .stringConf
      .createWithDefault("")
}
