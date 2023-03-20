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

package org.apache.spark.scheduler

import org.apache.spark.shuffle.rss.RssUtils

//class RssStageRetryHook extends StageRetryHook {

  /**
   * If RSS is the shuffle manager, call registerShuffle() on shuffle dependency so that
   * a fresh list of RSS servers is picked.
   */
//  override def beforeStageRetryHookSync(stageToBeRetried: Stage): Unit = {
//    stageToBeRetried match {
//      case stage: ShuffleMapStage if RssUtils.isUsingRSSForShuffle
//      (stage.shuffleDep.shuffleHandle, stage.rdd.conf) =>
//        StageRetryHookMetrics.getInstance(stage.rdd.conf).incStageRetryHookTriggeredMetrics(stage.id)
//        stage.shuffleDep.registerShuffle()
//      case _ =>
//    }
//  }
//}
