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

import org.apache.spark.executor.{ShuffleWriteMetrics, TempShuffleReadMetrics}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.helper.S3ShuffleHelper
import org.apache.spark.shuffle.sort.S3SortShuffleWriter
import org.apache.spark.storage.{BlockManager, BlockManagerId, S3ShuffleReader}
import org.apache.spark.util.Utils
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf, SparkContext, SparkEnv, TaskContext, TaskContextImpl}
import org.junit.Test
import org.mockito.Mockito.{mock, when}

import java.util.{Properties, UUID}

/**
 * This test is based on Spark "SortShuffleWriterSuite.scala".
 */
class S3SortShuffleTest {

  @Test
  def testShuffle() = {
    val conf = newSparkConf()
    val sc = new SparkContext(conf)

    val context = fakeTaskContext(sc.env)

    val shuffleId = 0
    val numMaps = 5

    val serializer = new KryoSerializer(conf)
    val partitioner = new Partitioner() {
      def numPartitions = numMaps

      def getPartition(key: Any) = Utils.nonNegativeMod(key.hashCode, numPartitions)
    }

    val blockManager: BlockManager = mock(classOf[BlockManager])
    when(blockManager.shuffleServerId).thenReturn(BlockManagerId("test", "test", 400))
    val shuffleHandle: BaseShuffleHandle[Int, Int, Int] = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.partitioner).thenReturn(partitioner)
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }

    val records = List[(Int, Int)]((1, 2), (2, 3), (4, 4), (6, 5))
    val mapId = 1
    val metrics = new ShuffleWriteMetrics
    val writer = new S3SortShuffleWriter[Int, Int, Int](
      conf,
      blockManager,
      shuffleHandle,
      mapId, context, metrics
      )
    writer.write(records.toIterator)
    writer.stop(success = true)

    val partitionLengths = S3ShuffleHelper.getPartitionLengthsCached(shuffleId, mapId)

    val readMetrics = new TempShuffleReadMetrics()
    val reader = new S3ShuffleReader[Int, Int](
      conf,
      shuffleHandle,
      context,
      readMetrics,
      startMapIndex = mapId,
      endMapIndex = mapId,
      startPartition = 0,
      endPartition = partitioner.numPartitions,
      shouldBatchFetch = true
      )

    val recordsRead = reader.read().toSeq
    assert(recordsRead.size == records.size)
  }

  def newSparkConf(): SparkConf = new SparkConf()
    .setAppName("testApp")
    .setMaster(s"local[2]")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.app.id", "app-" + UUID.randomUUID())
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.access.key", sys.env("AWS_ACCESS_KEY_ID"))
    .set("spark.hadoop.fs.s3a.secret.key", sys.env("AWS_SECRET_ACCESS_KEY"))
    .set("spark.hadoop.fs.s3a.endpoint", sys.env("S3_ENDPOINT_URL"))
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", sys.env("S3_ENDPOINT_USE_SSL"))
    .set("spark.shuffle.s3.useBlockManager", "false")
    .set("spark.shuffle.s3.rootDir", sys.env("S3_SHUFFLE_ROOT"))
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.local.dir", "./spark-temp") // Configure the working dir.
    .set("spark.shuffle.sort.io.plugin.class", "org.apache.spark.shuffle.S3ShuffleDataIO")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.S3ShuffleManager")
    .set("spark.shuffle.s3.forceBypassMergeSort", "false")
    .set("spark.shuffle.s3.cleanup", "false") // Avoid issues with cleanup.

  def fakeTaskContext(env: SparkEnv): TaskContext = {
    val taskMemoryManager = new TaskMemoryManager(env.memoryManager, 0)
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = 0,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = env.metricsSystem)
  }
}
