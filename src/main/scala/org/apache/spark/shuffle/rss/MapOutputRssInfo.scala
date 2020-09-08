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

/**
 * This class stores RSS information which is retrieved from map output tracker
 * @param numMaps
 * @param numRssServers
 * @param latestStageAttemptNumber
 * @param latestTaskAttemptIds
 */
case class MapOutputRssInfo(numMaps: Int, numRssServers: Int, latestStageAttemptNumber: Int, latestTaskAttemptIds: Array[Long]) {
  override def toString: String = {
    val latestTaskAttemptIdsStr = latestTaskAttemptIds.mkString(",")
    s"MapOutputRssInfo(numMaps: $numMaps, numRssServers: $numRssServers, latestStageAttemptNumber: $latestStageAttemptNumber, latestTaskAttemptIds: $latestTaskAttemptIdsStr)"
  }
}
