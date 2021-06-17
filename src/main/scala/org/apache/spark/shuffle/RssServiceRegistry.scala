/*
 * Copyright (c) 2021 Uber Technologies, Inc.
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

import com.uber.rss.exceptions.RssException
import com.uber.rss.metadata.{ServiceRegistry, StandaloneServiceRegistryClient, ZooKeeperServiceRegistry}
import com.uber.rss.util.ServerHostAndPort
import com.uber.rss.StreamServerConfig
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

object RssServiceRegistry extends Logging {
  private[this] var sparkConfOpts: Option[SparkConf] = None

  def init(conf: SparkConf): Unit = synchronized {
    logInfo("Initializing RssServiceRegistry's SparkConf.")
    sparkConfOpts = Some(conf)
  }

  def sparkConf: SparkConf = {
    if(sparkConfOpts.isEmpty) {
      logError("SparkConf not Initialized. Call 'RssServiceRegistry.init' before calling this method.")
      throw new IllegalStateException("RssServiceRegistry object not Initialized.")
    }
    sparkConfOpts.get
  }

  private def getZooKeeperServers: String = {
    val serversValue = sparkConf.get(RssOpts.serviceRegistryZKServers)
    serversValue
  }

  def getDataCenter: String = {
    var dataCenterValue = sparkConf.get(RssOpts.dataCenter)
    if (StringUtils.isBlank(dataCenterValue)) {
      dataCenterValue = StreamServerConfig.DEFAULT_DATA_CENTER;
    }
    dataCenterValue
  }

  private def createServiceRegistry: ServiceRegistry = {
    val serviceRegistryType = sparkConf.get(RssOpts.serviceRegistryType)
    val networkTimeoutMillis = sparkConf.get(RssOpts.networkTimeout).toInt
    val networkRetries = sparkConf.get(RssOpts.networkRetries).toInt

    logInfo(s"Service registry type: $serviceRegistryType")

    serviceRegistryType match {
      case ServiceRegistry.TYPE_ZOOKEEPER =>
        val zkServers = getZooKeeperServers
        ZooKeeperServiceRegistry.createTimingInstance(zkServers, networkTimeoutMillis, networkRetries)
      case ServiceRegistry.TYPE_STANDALONE =>
        val serviceRegistryServer = sparkConf.get(RssOpts.serviceRegistryServer)
        if (StringUtils.isBlank(serviceRegistryServer)) {
          throw new RssException(s"${RssOpts.serviceRegistryServer.key} configure is not set")
        }
        val hostAndPort = ServerHostAndPort.fromString(serviceRegistryServer)
        new StandaloneServiceRegistryClient(hostAndPort.getHost, hostAndPort.getPort, networkTimeoutMillis, "rss")
      case _ => throw new IllegalArgumentException(s"Invalid service registry type: $serviceRegistryType" )
    }
  }

  def executeWithServiceRegistry[R](callback:(ServiceRegistry) => R): R = {
    var serviceRegistry: Option[ServiceRegistry] = None
    try {
      serviceRegistry = Some(createServiceRegistry)
      callback(serviceRegistry.get)
    } finally {
      if(serviceRegistry.isDefined) {
        serviceRegistry.get.close()
      }
    }
  }
}
