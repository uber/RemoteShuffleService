package org.apache.spark.shuffle.rss

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
