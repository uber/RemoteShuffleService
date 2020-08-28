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

import java.util
import java.util.Random
import java.util.function.Supplier

import com.google.common.net.HostAndPort
import com.uber.rss.{RssBuildInfo, StreamServerConfig}
import com.uber.rss.clients.{MultiServerAsyncWriteClient, MultiServerSyncWriteClient, MultiServerWriteClient, PooledWriteClientFactory, ServerConnectionCacheUpdateRefresher, ServerConnectionStringCache, ServerConnectionStringResolver, ServerReplicationGroupUtil, ShuffleWriteConfig}
import com.uber.rss.common.{AppShuffleId, AppTaskAttemptId, ServerDetail, ServerList}
import com.uber.rss.exceptions.{RssException, RssInvalidStateException, RssNoServerAvailableException, RssServerResolveException}
import com.uber.rss.metadata.{ServiceRegistry, ServiceRegistryUtils, StandaloneServiceRegistryClient, ZooKeeperServiceRegistry}
import com.uber.rss.metrics.{M3Stats, ShuffleClientStageMetrics, ShuffleClientStageMetricsKey}
import com.uber.rss.util.{ExceptionUtils, RetryUtils, ThreadUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rss.{RssSparkListener, RssUtils}
import org.apache.spark.shuffle.sort.SortShuffleManager

import scala.collection.JavaConverters

object RssShuffleManager {
  val numFallbackSortShuffle = M3Stats.getDefaultScope.counter("numFallbackSortShuffle")
}

class RssShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  logInfo(s"Creating ShuffleManager instance: ${this.getClass.getSimpleName}, version: ${RssBuildInfo.Version}, revision: ${RssBuildInfo.Revision}")

  private val SparkYarnQueueConfigKey = "spark.yarn.queue"
  private val NumRssServersMetricName = "numRssServers2"
  private val FailToGetRssServersMetricName = "failToGetRssServers2"
  private val RssDataCenterTagName = "rssDataCenter"
  private val RssClusterTagName = "rssCluster"
  private val UserMetricTagName = "user"

  private val networkTimeoutMillis = conf.get(RssOpts.networkTimeout).toInt
  private val networkRetries = conf.get(RssOpts.networkRetries).toInt
  private val pollInterval = conf.get(RssOpts.pollInterval)
  private val dataAvailableWaitTime = conf.get(RssOpts.readerDataAvailableWaitTime)
  
  private var shuffleClientStageMetrics: ShuffleClientStageMetrics = null

  private val serviceRegistry = createServiceRegistry
  private val dataCenter = getDataCenter
  private val cluster = conf.get(RssOpts.cluster)

  private val rssCompressionBufferSize = conf.get(RssOpts.compressionBufferSize)

  // This is for test support only
  private var fallbackToOriginalSparkShuffleTestFlag = false
  private var sortShuffleManager: SortShuffleManager = null
  def fallbackToOriginalSparkShuffle(fallback: Boolean): Unit = {
    if (fallback) {
      RssShuffleManager.numFallbackSortShuffle.inc(1)
    }
    fallbackToOriginalSparkShuffleTestFlag = fallback
  }

  private def getSparkContext = {
    SparkContext.getActive.get
  }
  
  // This method is called in Spark driver side, and Spark driver will make some decision, e.g. determining what 
  // RSS servers to use. Then Spark driver will return a ShuffleHandle and pass that ShuffleHandle to executors (getWriter/getReader).
  override def registerShuffle[K, V, C](shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // RSS does not support speculation yet, due to the random task attempt ids (finished map task attempt id not always increasing).
    // We will fall back to SortShuffleManager if speculation is configured to true.
    val useSpeculation = conf.getBoolean("spark.speculation", false)
    if (fallbackToOriginalSparkShuffleTestFlag || useSpeculation) {
      if (sortShuffleManager == null) {
        sortShuffleManager = new SortShuffleManager(conf)
      }
      logInfo(s"Use ShuffleManager: ${sortShuffleManager.getClass().getSimpleName()}")
      return sortShuffleManager.registerShuffle(shuffleId, numMaps, dependency)
    }

    logInfo(s"Use ShuffleManager: ${this.getClass().getSimpleName()}")

    val numPartitions = dependency.partitioner.numPartitions;

    val sparkContext = getSparkContext

    val user = sparkContext.sparkUser
    val queue = conf.get(SparkYarnQueueConfigKey, "")

    val appId = conf.getAppId
    val appAttempt = sparkContext.applicationAttemptId.getOrElse("0")

    var rssServerSelectionResult: RssServerSelectionResult = null

    val excludeHostsConfigValue = conf.get(RssOpts.excludeHosts)
    val excludeHosts = excludeHostsConfigValue.split(",").filter(!_.isEmpty).distinct

    try {
      rssServerSelectionResult = getRssServers(numMaps, numPartitions, excludeHosts)
      val rssServers = rssServerSelectionResult.servers
      logInfo(s"Selected ${rssServers.size} RSS servers for shuffle $shuffleId, maps: $numMaps, partitions: $numPartitions, replicas: ${rssServerSelectionResult.replicas}, partition fanout: ${rssServerSelectionResult.partitionFanout}, ${rssServers.mkString(",")}")

      val tagMap = new java.util.HashMap[String, String]()
      tagMap.put(RssDataCenterTagName, dataCenter)
      tagMap.put(RssClusterTagName, cluster)
      tagMap.put(UserMetricTagName, user)
      M3Stats.getDefaultScope.tagged(tagMap).gauge(NumRssServersMetricName).update(rssServers.length)
    } catch {
      case e: Throwable => {
        M3Stats.addException( e, this.getClass().getSimpleName() )

        val tagMap = new java.util.HashMap[String, String]()
        tagMap.put(RssDataCenterTagName, dataCenter)
        tagMap.put(RssClusterTagName, cluster)
        tagMap.put(UserMetricTagName, user)
        M3Stats.getDefaultScope.tagged(tagMap).counter(FailToGetRssServersMetricName).inc(1)

        logWarning( s"Fallback to sort shuffle because failed to get remote shuffle servers", e )
        fallbackToOriginalSparkShuffle( true )
        sortShuffleManager = new SortShuffleManager( conf )
        return sortShuffleManager.registerShuffle( shuffleId, numMaps, dependency )
      }
    }

    RssSparkListener.registerSparkListenerOnlyOnce(sparkContext, () =>
      new RssSparkListener(
        user,
        conf.getAppId,
        appAttempt,
        rssServerSelectionResult.servers.map(_.getConnectionString()),
        networkTimeoutMillis))

    val shuffleClientStageMetricsKey = new ShuffleClientStageMetricsKey(user, queue)
    shuffleClientStageMetrics = new ShuffleClientStageMetrics(shuffleClientStageMetricsKey)

    shuffleClientStageMetrics.getNumRegisterShuffle.inc(1)
    shuffleClientStageMetrics.getNumMappers().recordValue(numMaps)
    shuffleClientStageMetrics.getNumReducers().recordValue(numPartitions)
    
    val dependencyInfo = s"numPartitions: ${dependency.partitioner.numPartitions}, " +
      s"serializer: ${dependency.serializer.getClass().getSimpleName()}, " +
      s"keyOrdering: ${dependency.keyOrdering}, " +
      s"aggregator: ${dependency.aggregator}, " +
      s"mapSideCombine: ${dependency.mapSideCombine}, " +
      s"keyClassName: ${dependency.keyClassName}, " +
      s"valueClassName: ${dependency.valueClassName}"

    logInfo(s"registerShuffle: $appId, $appAttempt, $shuffleId, $numMaps, $dependencyInfo")

    val rssServerHandles = rssServerSelectionResult.servers.map(t => new RssShuffleServerHandle(t.getServerId(), t.getRunningVersion(), t.getConnectionString())).toArray
    new RssShuffleHandle(shuffleId, appId, appAttempt, numMaps, user, queue, dependency, rssServerHandles, rssServerSelectionResult.partitionFanout)
  }

  // This method is called in Spark executor, getting information from Spark driver via the ShuffleHandle.
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    if (checkShuffleHandleAndFallbackToOriginalSparkShuffle(handle)) {
      logInfo(s"getWriter: Use ShuffleManager: ${sortShuffleManager.getClass().getSimpleName()}, $handle, mapId: $mapId")
      return sortShuffleManager.getWriter(handle, mapId, context)
    }

    val stageAttemptNumber = context.stageAttemptNumber()
    logInfo(s"getWriter: Use ShuffleManager: ${this.getClass().getSimpleName()}, $handle, mapId: $mapId, stageId: ${context.stageId()}, stageAttemptNumber: $stageAttemptNumber, shuffleId: ${handle.shuffleId}")

    handle match {
      case rssShuffleHandle: RssShuffleHandle[K@unchecked, V@unchecked, _] => {
        val writerQueueSize = conf.get(RssOpts.writerQueueSize)

        val mapInfo = new AppTaskAttemptId(
          conf.getAppId,
          rssShuffleHandle.appAttempt,
          handle.shuffleId,
          mapId,
          context.taskAttemptId()
        )

        logDebug( s"getWriter $mapInfo" )

        createShuffleClientStageMetricsIfNeeded( rssShuffleHandle )

        val serializer = SparkEnv.get.serializer.newInstance()
        val maxWaitMillis = conf.get( RssOpts.maxWaitTime )
        val useConnectionPool = conf.get(RssOpts.useConnectionPool)
        val rssFileCompressionCodec = conf.get(RssOpts.fileCompressionCodec)
        val rssMapsPerSplit = conf.get(RssOpts.mapsPerSplit)
        var rssNumSplits = Math.ceil(rssShuffleHandle.numMaps.toDouble/rssMapsPerSplit.toDouble).toInt
        val rssMinSplits = conf.get(RssOpts.minSplits)
        val rssMaxSplits = conf.get(RssOpts.maxSplits)
        if (rssNumSplits < rssMinSplits) {
          rssNumSplits = rssMinSplits
        } else if (rssNumSplits > rssMaxSplits) {
          rssNumSplits = rssMaxSplits
        }
        val shuffleWriteConfig = new ShuffleWriteConfig(rssFileCompressionCodec, rssNumSplits.toShort)
        val rssReplicas = conf.get(RssOpts.replicas)
        if (rssReplicas <= 0) {
          throw new RssException(s"Invalid config value for ${RssOpts.replicas.key}: $rssReplicas")
        }
        val rssServers: ServerList = ServerConnectionStringCache.getInstance().getServerList(rssShuffleHandle.getServerList)
        val serverReplicationGroups = ServerReplicationGroupUtil.createReplicationGroups(rssServers.getSevers, rssReplicas)

        val serverConnectionResolver = new ServerConnectionStringResolver {
          override def resolveConnection(serverId: String): ServerDetail = {
            val serverDetailInShuffleHandle = rssShuffleHandle.getServerList.getSeverDetail(serverId)
            if (serverDetailInShuffleHandle == null) {
              throw new FetchFailedException(
                bmAddress = RssUtils.createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId, stageAttemptNumber),
                shuffleId = rssShuffleHandle.shuffleId,
                mapId = -1,
                reduceId = 0,
                message = s"Failed to get server detail for $serverId from shuffle handle: $rssShuffleHandle")
            }
            // random sleep some time to avoid request spike on service registry
            val random = new Random()
            val randomWaitMillis = random.nextInt(pollInterval)
            ThreadUtils.sleep(randomWaitMillis)
            val lookupResult = serviceRegistry.lookupServers(dataCenter, cluster, util.Arrays.asList(serverId))
            if (lookupResult == null) {
              throw new RssServerResolveException(s"Got null when looking up server for $serverId")
            }
            if (lookupResult.size() != 1) {
              throw new RssInvalidStateException(s"Invalid result $lookupResult when looking up server for $serverId")
            }
            val refreshedServer: ServerDetail = lookupResult.get(0)
            ServerConnectionStringCache.getInstance().updateServer(serverId, refreshedServer)
            if (!refreshedServer.equals(serverDetailInShuffleHandle)) {
              throw new FetchFailedException(
                bmAddress = RssUtils.createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId, stageAttemptNumber),
                shuffleId = rssShuffleHandle.shuffleId,
                mapId = -1,
                reduceId = 0,
                message = s"Detected server restart, current server: $refreshedServer, previous server: $serverDetailInShuffleHandle")
            }
            refreshedServer
          }
        }
        val serverConnectionRefresher = new ServerConnectionCacheUpdateRefresher(serverConnectionResolver, ServerConnectionStringCache.getInstance())

        val writerAsyncFinish = conf.get(RssOpts.writerAsyncFinish)
        val finishUploadAck = !writerAsyncFinish

        RetryUtils.retry(pollInterval, pollInterval * 10, maxWaitMillis, "create write client", new Supplier[ShuffleWriter[K, V]] {
          override def get(): ShuffleWriter[K, V] = {
            val writeClient: MultiServerWriteClient =
              if (writerQueueSize == 0) {
                logInfo(s"Use replicated sync writer, $rssNumSplits splits, ${rssShuffleHandle.partitionFanout} partition fanout, $serverReplicationGroups, finishUploadAck: $finishUploadAck")
                new MultiServerSyncWriteClient(
                  serverReplicationGroups,
                  rssShuffleHandle.partitionFanout,
                  networkTimeoutMillis,
                  maxWaitMillis,
                  serverConnectionRefresher,
                  finishUploadAck,
                  useConnectionPool,
                  rssCompressionBufferSize,
                  rssShuffleHandle.user,
                  rssShuffleHandle.appId,
                  rssShuffleHandle.appAttempt,
                  shuffleWriteConfig)
              } else {
                val maxThreads = conf.get(RssOpts.writerMaxThreads)
                val serverThreadRatio = 8.0
                val numThreadsBasedOnShuffleServers = Math.ceil(rssShuffleHandle.rssServers.length.toDouble/serverThreadRatio)
                val numThreads = Math.min(numThreadsBasedOnShuffleServers, maxThreads).toInt
                logInfo(s"Use replicated async writer with queue size $writerQueueSize threads $numThreads, $rssNumSplits splits, ${rssShuffleHandle.partitionFanout} partition fanout, $serverReplicationGroups, finishUploadAck: $finishUploadAck")
                new MultiServerAsyncWriteClient(
                  serverReplicationGroups,
                  rssShuffleHandle.partitionFanout,
                  networkTimeoutMillis,
                  maxWaitMillis,
                  serverConnectionRefresher,
                  finishUploadAck,
                  useConnectionPool,
                  rssCompressionBufferSize,
                  writerQueueSize,
                  numThreads,
                  rssShuffleHandle.user,
                  rssShuffleHandle.appId,
                  rssShuffleHandle.appAttempt,
                  shuffleWriteConfig)
              }

            try {
              writeClient.connect()

              new RssShuffleWriter(
                rssShuffleHandle.user,
                new ServerList(rssShuffleHandle.rssServers.map(_.toServerDetail()).toArray),
                writeClient,
                mapInfo,
                rssShuffleHandle.numMaps,
                serializer,
                stageAttemptNumber,
                rssShuffleHandle.dependency,
                shuffleClientStageMetrics,
                context.taskMetrics().shuffleWriteMetrics)
            } catch {
              case ex: Throwable => {
                ExceptionUtils.closeWithoutException(writeClient)
                throw ex
              }
            }
          }
        })
      }
    }
  }

  // This method is called in Spark executor, getting information from Spark driver via the ShuffleHandle.
  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    if (checkShuffleHandleAndFallbackToOriginalSparkShuffle(handle)) {
      logInfo(s"getReader: Use ShuffleManager: ${sortShuffleManager.getClass().getSimpleName()}, $handle, partitions: [$startPartition, $endPartition)")
      return sortShuffleManager.getReader(handle, startPartition, endPartition, context)
    }

    logInfo(s"getReader: Use ShuffleManager: ${this.getClass().getSimpleName()}, $handle, partitions: [$startPartition, $endPartition)")

    val rssShuffleHandle = handle.asInstanceOf[RssShuffleHandle[K, _, C]]
    val shuffleInfo = new AppShuffleId(
      conf.getAppId,
      rssShuffleHandle.appAttempt,
      handle.shuffleId
    )

    if (rssShuffleHandle.numMaps == 0) {
      return new RssEmptyShuffleReader(
        shuffleInfo,
        startPartition,
        endPartition)
    }

    val queueSize = conf.get(RssOpts.readerQueueSize)

    // TODO use serializer from depedency.serializer, which may be UnsafeRowSerializerInstance and does not implement method to serialize single object
    val serializer = SparkEnv.get.serializer.newInstance()
    val rssReplicas = conf.get(RssOpts.replicas)
    val rssCheckReplicaConsistency = conf.get(RssOpts.checkReplicaConsistency)
    val maxWaitMillis = conf.get(RssOpts.maxWaitTime)
    val rssServers = ServerConnectionStringCache.getInstance().getServerList(rssShuffleHandle.getServerList)
    new RssShuffleReader(
      user = rssShuffleHandle.user,
      shuffleInfo = shuffleInfo,
      startPartition = startPartition,
      endPartition = endPartition,
      serializer = serializer,
      context = context,
      shuffleDependency = rssShuffleHandle.dependency,
      numMaps = rssShuffleHandle.numMaps,
      rssServers = rssServers,
      partitionFanout = rssShuffleHandle.partitionFanout,
      serviceRegistry = serviceRegistry,
      serviceRegistryDataCenter = dataCenter,
      serviceRegistryCluster = cluster,
      timeoutMillis = networkTimeoutMillis,
      maxRetryMillis = maxWaitMillis.toInt,
      dataAvailablePollInterval = pollInterval,
      dataAvailableWaitTime = dataAvailableWaitTime,
      dataCompressed = rssCompressionBufferSize > 0,
      queueSize = queueSize,
      shuffleReplicas = rssReplicas,
      checkShuffleReplicaConsistency = rssCheckReplicaConsistency)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (sortShuffleManager != null) {
      return sortShuffleManager.unregisterShuffle(shuffleId)
    }
    
    if (shuffleClientStageMetrics != null) {
      shuffleClientStageMetrics.close()
    }
    
    true
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    if (sortShuffleManager != null) {
      return sortShuffleManager.shuffleBlockResolver
    }
    
    new RssShuffleBlockResolver()
  }

  override def stop(): Unit = {
    if (sortShuffleManager != null) {
      sortShuffleManager.stop()
    }
    PooledWriteClientFactory.getInstance().shutdown();
    serviceRegistry.close()
    M3Stats.closeDefaultScope()
  }

  private def createServiceRegistry: ServiceRegistry = {
    val serviceRegistryType = conf.get(RssOpts.serviceRegistryType)
    logInfo(s"Service registry type: $serviceRegistryType")

    serviceRegistryType match {
      case ServiceRegistry.TYPE_ZOOKEEPER =>
        val zkServers = getZooKeeperServers
        ZooKeeperServiceRegistry.createTimingInstance(zkServers, networkTimeoutMillis, networkRetries)
      case ServiceRegistry.TYPE_STANDALONE =>
        val serviceRegistryServer = conf.get(RssOpts.serviceRegistryServer)
        if (serviceRegistryServer == null || serviceRegistryServer.isEmpty) {
          throw new RssException(s"${RssOpts.serviceRegistryServer.key} configure is not set")
        }
        val hostAndPort = HostAndPort.fromString(serviceRegistryServer)
        new StandaloneServiceRegistryClient(hostAndPort.getHostText, hostAndPort.getPort, networkTimeoutMillis, "rss")
      case _ => throw new RuntimeException(s"Invalid service registry type: $serviceRegistryType" )
    }
  }

  private def getDataCenter: String = {
    var dataCenterValue = conf.get(RssOpts.dataCenter)
    if (StringUtils.isBlank(dataCenterValue)) {
      dataCenterValue = StreamServerConfig.DEFAULT_DATA_CENTER;
    }
    dataCenterValue
  }

  private def getZooKeeperServers: String = {
    val serversValue = conf.get(RssOpts.serviceRegistryZKServers)
    serversValue
  }

  private def getRssServers(numMaps: Int, numPartitions: Int, excludeHosts: Seq[String]): RssServerSelectionResult = {
    val maxServerCount = conf.get(RssOpts.maxServerCount)
    val minServerCount = conf.get(RssOpts.minServerCount)

    var selectedServerCount = maxServerCount

    val shuffleServerRatio = conf.get(RssOpts.serverRatio)
    val serverCountEstimate = Math.ceil(Math.max(numMaps, numPartitions).doubleValue()/shuffleServerRatio).intValue()
    if (selectedServerCount > serverCountEstimate) {
      selectedServerCount = serverCountEstimate
    }

    if (selectedServerCount > numPartitions) {
      selectedServerCount = numPartitions
    }

    if (selectedServerCount <= 0) {
      selectedServerCount = 1
    }

    val rssReplicas = conf.get(RssOpts.replicas)
    selectedServerCount = selectedServerCount * rssReplicas

    if (selectedServerCount < minServerCount) {
      selectedServerCount = minServerCount
    }

    val excludeHostsJavaCollection = JavaConverters.asJavaCollectionConverter(excludeHosts).asJavaCollection
    val servers = ServiceRegistryUtils.getReachableServers(serviceRegistry, selectedServerCount, networkTimeoutMillis, dataCenter, cluster, excludeHostsJavaCollection)
    if (servers.isEmpty) {
      throw new RssNoServerAvailableException("There is no reachable RSS server")
    }

    val serverArray = servers.toArray(new Array[ServerDetail](0))

    var partitionFanout = 1
    if (minServerCount > 1) {
      // if min server count is configured, try to distribute a single partition on multiple servers
      val numReplicationGroups = serverArray.length / rssReplicas
      if (numReplicationGroups > numPartitions) {
        partitionFanout = numReplicationGroups / numPartitions
      }
    }

    RssServerSelectionResult(serverArray, rssReplicas, partitionFanout)
  }

  private def createShuffleClientStageMetricsIfNeeded(rssShuffleHandle: RssShuffleHandle[_, _, _]) = {
    if (shuffleClientStageMetrics == null) {
      val shuffleClientStageMetricsKey = new ShuffleClientStageMetricsKey(rssShuffleHandle.user, rssShuffleHandle.queue)
      shuffleClientStageMetrics = new ShuffleClientStageMetrics(shuffleClientStageMetricsKey)
    }
  }
  
  private def checkShuffleHandleAndFallbackToOriginalSparkShuffle(handle: ShuffleHandle) =  {
    // Fallback to original Spark sort shuffle if handle is not RssShuffleHandle
    val needFallback = !handle.isInstanceOf[RssShuffleHandle[_, _, _]]
    if (needFallback) {
      if (sortShuffleManager == null) {
        sortShuffleManager = new SortShuffleManager(conf)
      }
    }
    needFallback
  }
}
