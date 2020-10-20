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

package com.uber.rss.test

import java.util.Collections
import java.util.function.Consumer

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object SparkSqlShuffleTestApp {

  def main (args: Array[String]): Unit = {
    var queryFile = ""
    var verify = "count"
    var numIterations = 1
    
    var i = 0
    while (i < args.length) {
      val argName = args( i )
      i += 1
      if (argName.equalsIgnoreCase( "-queryFile" )) {
        queryFile = args(i)
        i += 1
      } else if (argName.equalsIgnoreCase( "-verify" )) {
        verify = args(i)
        i += 1
      } else if (argName.equalsIgnoreCase( "-numIterations" )) {
        numIterations = args(i).toInt
        i += 1
      } else {
        throw new RuntimeException( s"Invalid argument: $argName" )
      }
    }
    
    if (queryFile.isEmpty()) {
      throw new RuntimeException("-queryFile argument is missing")
    }
    
    var conf = new SparkConf()
      .setAppName(SparkSqlShuffleTestApp.getClass().getSimpleName())
    
    var spark: SparkSession = null

    spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    if (queryFile.startsWith("./")) {
      val currentDirectory = new java.io.File(".").getCanonicalPath
      queryFile = "file://" + currentDirectory + "/" + queryFile
    }

    val queryFilePath = new Path(queryFile)
    val fs = queryFilePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    
    if (!fs.exists(queryFilePath)) {
      System.out.println(s"Query file $queryFile does not exist. Stop the application.")
      spark.stop()
      return
    }
    
    val queryFiles = scala.collection.mutable.ListBuffer.empty[Path]
    
    if (fs.isFile(queryFilePath)) {
      queryFiles += queryFilePath
    } else {
      val listFilesResultIterator = fs.listFiles( queryFilePath, true )
      while (listFilesResultIterator.hasNext()) {
        val fileStatus = listFilesResultIterator.next()
        queryFiles += fileStatus.getPath()
      }
    }

    for (path <- queryFiles) {
      System.out.println(s"Reading file: $path")
      val queries = new java.util.ArrayList[String]()
      val stream = fs.open(path)
      try {
        if (path.getName().toLowerCase().endsWith(".multi")) {
          val lines = IOUtils.readLines(stream);
          queries.addAll(lines)
          lines.forEach(new Consumer[String] {
            override def accept(t: String): Unit = {
              System.out.println(s"Multiple queries ($path): $t")
            }
          })
        } else {
          val text = IOUtils.toString(stream)
          System.out.println(s"Query text ($path): $text")
          queries.add(text)
        }
      } finally {
        stream.close()
      }

      System.out.println("Shuffling all queries to run them randomly")
      Collections.shuffle(queries)

      queries.forEach(new Consumer[String] {
        override def accept(queryText: String): Unit = {
          (0 until numIterations).foreach(i => {
            System.out.println("Running query first time: " + queryText)

            val startTime = System.currentTimeMillis()
            val rows: Array[Row] = spark.sql(queryText).collect()
            val durationSeconds = (System.currentTimeMillis() - startTime) / 1000

            System.out.println("Running query second time: " + queryText)

            val startTime2 = System.currentTimeMillis()
            val rows2: Array[Row] = spark.sql(queryText).collect()
            val durationSeconds2 = (System.currentTimeMillis() - startTime2) / 1000

            System.out.println(s"Comparing results, first run: ${rows.size} rows ($durationSeconds seconds), second run: ${rows2.size} rows ($durationSeconds2 seconds)")

            verifyResults(verify, rows, rows2)
          })
        }
      })
    }

    spark.stop()
  }

  private def verifyResults(verifyMode: String, result1: Array[Row], result2: Array[Row]): Unit = {
    if (verifyMode == null || verifyMode.isEmpty()) {
      System.out.println("Skip verify")
      return
    }

    if (verifyMode.equalsIgnoreCase("count")) {
      System.out.println("Verifying results by row count")
      verifyResultsByRowCount(result1, result2)
    } else if (verifyMode.equalsIgnoreCase("row")) {
      System.out.println("Verifying results by row object")
      verifyResultsByRowCount(result1, result2)
      verifyResultsByRowObject(result1, result2)
    } else if (verifyMode.toLowerCase().startsWith("column")) {
      val column = verifyMode.substring("column".size).toInt
      System.out.println(s"Verifying results by column $column")
      verifyResultsByRowCount(result1, result2)
      verifyResultsByColumn(column, result1, result2)
    } else {
      throw new RuntimeException(s"Invalid mode to verify: $verifyMode")
    }
  }

  private def verifyResultsByRowCount(result1: Array[Row], result2: Array[Row]): Unit = {
    if (result1.size != result2.size) {
      throw new RuntimeException(s"Row count is different: ${result1.size} VS ${result2.size}")
    }
  }

  private def verifyResultsByRowObject(result1: Array[Row], result2: Array[Row]): Unit = {
    (0 until result1.size).foreach(i => {
      val row = result1(i)
      val row2 = result2(i)
      if (!row.equals(row2)) {
        throw new RuntimeException(s"Row $i is different: $row VS $row2")
      }
    })
  }
  
  private def verifyResultsByColumn(column: Int, result1: Array[Row], result2: Array[Row]): Unit = {
    val values1 = result1.map(_.get(column)).map(String.valueOf).sorted.toList
    val values2 = result2.map(_.get(column)).map(String.valueOf).sorted.toList
    
    (0 until values1.size).foreach(i => {
      val v1 = values1(i)
      val v2 = values2(i)
      if (!v1.equals(v2)) {
        throw new RuntimeException(s"Row $i column $column (after sorting)is different: $v1 VS $v2")
      }
    })
  }
}
