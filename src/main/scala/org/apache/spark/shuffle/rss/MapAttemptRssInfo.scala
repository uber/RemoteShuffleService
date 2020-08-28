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

package org.apache.spark.shuffle.rss

import scala.collection.JavaConversions

import com.uber.rss.common.ServerList

/**
 * This class stores RSS information for a map task attempt.
 * @param mapId
 * @param taskAttemptId
 * @param stageAttemptNumber
 * @param rssServers
 */
case class MapAttemptRssInfo(mapId: Int, taskAttemptId: Long, stageAttemptNumber: Int, rssServers: ServerList) {
  override def toString: String = {
    val rssServersStr = JavaConversions.asScalaBuffer(rssServers.getSevers).mkString(",")
    s"MapAttemptRssInfo(mapId: $mapId, taskAttemptId: $taskAttemptId, stageAttemptNumber: $stageAttemptNumber, rssServers: $rssServersStr)"
  }
}
