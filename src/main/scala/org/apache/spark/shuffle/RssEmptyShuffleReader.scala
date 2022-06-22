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

import com.uber.rss.common.AppShuffleId
import org.apache.spark.internal.Logging

/** *
 * This is a shuffle reader returning zero record.
 * It is used when there is zero partitions for mapper side. So the reader could return
 * empty record iterator directly without connecting to shuffle server.
 *
 * @tparam K
 * @tparam C
 */
class RssEmptyShuffleReader[K, C](
                                   shuffleInfo: AppShuffleId,
                                   startPartition: Int,
                                   endPartition: Int) extends ShuffleReader[K, C] with Logging {

  logInfo(s"Using RssEmptyShuffleReader: ${
    this.getClass.getSimpleName
  }, shuffleInfo: $shuffleInfo, startPartition: $startPartition, endPartition: $endPartition")

  override def read(): Iterator[Product2[K, C]] = {
    logInfo(s"RssEmptyShuffleReader read")
    Iterator.empty
  }
}
