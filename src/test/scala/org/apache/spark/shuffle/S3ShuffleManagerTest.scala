/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

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

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.scalatest.Assertions._

import java.util.UUID

case class KeyClass()

case class ValueClass()

case class CombinerClass()

/*
 * The test has been adapted from the following pull request https://github.com/apache/spark/pull/34864/files .
 */
class S3ShuffleManagerTest {

  @Test
  def foldByKey(): Unit = {
    val conf = newSparkConf()
    runWithSparkConf(conf)
  }

  @Test
  def foldByKey_zeroBuffering(): Unit = {
    val conf = newSparkConf()
    conf.set("spark.reducer.maxSizeInFlight", "0")
    conf.set("spark.network.maxRemoteBlockSizeFetchToMem", "0")
    runWithSparkConf(conf)
  }

  @Test
  def runWithSparkConf_noMapSideCombine(): Unit = {
    val conf = newSparkConf()
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "1000")
    val sc = new SparkContext(conf)
    try {
      //  Test copied from: src/test/scala/org/apache/spark/shuffle/ShuffleDependencySuite.scala
      val rdd = sc.parallelize(1 to 5, 4)
                  .map(key => (KeyClass(), ValueClass()))
                  .groupByKey()
      val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
      assert(!dep.mapSideCombine, "Test requires that no map-side aggregator is defined")
      assert(dep.keyClassName == classOf[KeyClass].getName)
      assert(dep.valueClassName == classOf[ValueClass].getName)
    } finally {
      sc.stop()
    }
  }

  @Test
  def forceSortShuffle(): Unit = {
    val conf = newSparkConf()
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "1")
    val sc = new SparkContext(conf)
    try {
      val numValues = 10000
      val numMaps = 3

      val rdd = sc.parallelize(0 until numValues, numMaps)
                  .map(t => {
                    val rand = scala.util.Random
                    (t) -> rand.nextInt(numValues)
                  })
                  .sortBy(_._2, ascending = true)
      val result = rdd.collect()

      var previous = result(0)._2
      for (i <- result.indices) {
        val value = result(i)._2
        assert(value >= previous)
        previous = value
      }
    } finally {
      sc.stop()
    }
  }

  @Test
  def testCombineByKey(): Unit = {
    val conf = newSparkConf()
    val sc = new SparkContext(conf)
    try {
      val numValuesPerPartition = 100000
      val numPartitions = 20
      val dataset = sc.parallelize(0 until numPartitions, numPartitions).mapPartitionsWithIndex {
        case (index, _) =>
          Iterator.tabulate(numValuesPerPartition) { offset =>
            val key = offset
            val value = offset*index
            (key, value*2)
          }
      }

      def convert_value(v: Int) = {
        1
      }

      def count_values(x: Int, value: Int) = {
        value + 1
      }

      def add_values(x: Int, y: Int) = {
        x + y
      }

      val sumCount = dataset.combineByKey(convert_value, count_values, add_values)
      val averageByKey = sumCount.sortByKey().collect()
      assert(averageByKey.size == numValuesPerPartition)
      var index = 0
      for (i <- averageByKey) {
        val key = i._1
        val value = i._2
        assert(key == index)
        assert(value == numPartitions)
        index += 1
      }

    } finally {
      sc.stop()
    }
  }

  @Test
  def teraSortLike(): Unit = {
    val conf = newSparkConf()
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "1")
    val sc = new SparkContext(conf)
    try {
      val numValuesPerPartition = 10000
      val numPartitions = 5

      val dataset = sc.parallelize(0 until numPartitions).mapPartitionsWithIndex {
        case (index, _) =>
          val rand = scala.util.Random
          Iterator.tabulate(numValuesPerPartition) { offset =>
            val key = rand.nextInt()
            val value = rand.nextInt()
            (key, value)
          }
      }
      val sorted = dataset.sortByKey(true, numPartitions - 1)
      val result = sorted.collect()

      var previous = result(0)._1
      for (i <- result.indices) {
        val value = result(i)._1
        assert(value >= previous)
        previous = value
      }
    } finally {
      sc.stop()
    }
  }

  @Test
  def runWithSparkMeasure(): Unit = {
    val conf = newSparkConf()
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
    val stageMetrics = StageMetrics(spark)
    val result = stageMetrics.runAndMeasure {
      spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").take(1)
    }
    assert(result.map(r => r.getLong(0)).head === 1000000000)

    val timestamp = System.currentTimeMillis()
    stageMetrics.createStageMetricsDF(s"spark_measure_test_${timestamp}")
    val metrics = stageMetrics.aggregateStageMetrics(s"spark_measure_test_${timestamp}")
    // get all of the stats
    val (runTime, bytesRead, recordsRead, bytesWritten, recordsWritten) =
      metrics.select("elapsedTime", "bytesRead",
                     "recordsRead", "bytesWritten", "recordsWritten")
             .take(1)
             .map(r => (r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3),
               r.getLong(4))).head
    println(f"Elapsed: ${runTime}, bytesRead: ${bytesRead}, recordsRead: ${recordsRead}, bytesWritten ${bytesWritten}, recordsWritten: ${recordsWritten}")
    spark.stop()
    spark.close()
  }

  private def runWithSparkConf(conf: SparkConf) = {
    val sc = new SparkContext(conf)

    try {
      val numValues = 10000
      val numMaps = 3
      val numPartitions = 5

      val rdd = sc.parallelize(0 until numValues, numMaps)
                  .map(t => ((t / 2) -> (t * 2).longValue()))
                  .foldByKey(0, numPartitions)((v1, v2) => v1 + v2)
      val result = rdd.collect()

      assert(result.length === numValues / 2)

      for (i <- result.indices) {
        val key = result(i)._1
        val value = result(i)._2
        assert(key * 2 * 2 + (key * 2 + 1) * 2 === value)
      }

      val keys = result.map(_._1).distinct.sorted
      assert(keys.length === numValues / 2)
      assert(keys(0) === 0)
      assert(keys.last === (numValues - 1) / 2)
    } finally {
      sc.stop()
    }
  }

  def newSparkConf(): SparkConf = new SparkConf()
    .setAppName("testApp")
    .setMaster(s"local[2]")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.app.id", "app-" + UUID.randomUUID())
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.access.key", sys.env("AWS_ACCESS_KEY_ID"))
    .set("spark.hadoop.fs.s3a.secret.key", sys.env("AWS_SECRET_ACCESS_KEY"))
    .set("spark.hadoop.fs.s3a.endpoint", sys.env("S3_ENDPOINT_URL"))
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", sys.env("S3_ENDPOINT_USE_SSL"))
    .set("spark.shuffle.s3.rootDir", sys.env("S3_SHUFFLE_ROOT"))
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.local.dir", "./spark-temp") // Configure the working dir.
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.S3ShuffleManager")
    .set("spark.shuffle.sort.io.plugin.class", "org.apache.spark.shuffle.sort.io.S3ShuffleDataIO")
    .set("spark.shuffle.s3.forceBypassMergeSort", "false")
}
