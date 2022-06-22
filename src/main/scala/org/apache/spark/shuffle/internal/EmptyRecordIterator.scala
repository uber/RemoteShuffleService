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

import com.uber.rss.exceptions.RssException
import org.apache.spark.internal.Logging

class EmptyRecordIterator[K, C]() extends Iterator[Product2[K, C]] with Logging {

  override def hasNext: Boolean = {
    false
  }

  override def next(): Product2[K, C] = {
    throw new RssException("Cannot get next element on empty iterator")
  }
}
