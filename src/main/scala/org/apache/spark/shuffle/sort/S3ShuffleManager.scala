//
// Copyright 2022- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache 2.0
//

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

package org.apache.spark.shuffle.sort

import com.ibm.SparkS3ShuffleBuild
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark._
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.helper.{S3ShuffleDispatcher, S3ShuffleHelper}
import org.apache.spark.storage.S3ShuffleReader

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/** This class was adapted from Apache Spark: SortShuffleManager.scala
  */
private[spark] class S3ShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  val versionString = s"${SparkS3ShuffleBuild.name}-${SparkS3ShuffleBuild.version} " +
    s"for ${SparkS3ShuffleBuild.sparkVersion}_${SparkS3ShuffleBuild.scalaVersion}"
  logInfo(s"Configured S3ShuffleManager (${versionString}).")
  private lazy val dispatcher = S3ShuffleDispatcher.get
  private lazy val shuffleExecutorComponents = S3ShuffleManager.loadShuffleExecutorComponents(conf)

  /** A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
    */
  override lazy val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)
  private val registeredShuffleIds = new mutable.HashSet[Int]()

  /** Obtains a [[ShuffleHandle]] to pass to tasks.
    */
  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    registeredShuffleIds.add(shuffleId)
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      logInfo(f"Using BypassMergeSortShuffleWriter for ${shuffleId}")
      new BypassMergeSortShuffleHandle[K, V](shuffleId, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      logInfo(f"Using UnsafeShuffleWriter for ${shuffleId}")
      new SerializedShuffleHandle[K, V](shuffleId, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      logInfo(f"Using SortShuffleWriter for ${shuffleId}")
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, dependency)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter
  ): ShuffleReader[K, C] = {
    if (dispatcher.useSparkShuffleFetch) {
      val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
        handle.shuffleId,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition
      )
      val canEnableBatchFetch = true
      return new BlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        blocksByAddress,
        context,
        metrics,
        shouldBatchFetch =
          canEnableBatchFetch && SortShuffleManager.canUseBatchFetch(startPartition, endPartition, context)
      )
    }
    new S3ShuffleReader(
      conf,
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      context,
      metrics,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      shouldBatchFetch = SortShuffleManager.canUseBatchFetch(startPartition, endPartition, context)
    )
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter
  ): ShuffleWriter[K, V] = {
    val env = SparkEnv.get
    val writer = handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          metrics,
          shuffleExecutorComponents
        )
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          bypassMergeSortHandle,
          mapId,
          env.conf,
          metrics,
          shuffleExecutorComponents
        )
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(other, mapId, context, metrics, shuffleExecutorComponents)
    }
    new S3ShuffleWriter[K, V](writer)
  }

  def purgeCaches(shuffleId: Int): Unit = {
    // Remove and close all input streams.
    dispatcher.closeCachedBlocks(shuffleId)
    // Remove metadata.
    S3ShuffleHelper.purgeCachedDataForShuffle(shuffleId)
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo(f"Unregister shuffle ${shuffleId}")
    registeredShuffleIds.remove(shuffleId)

    purgeCaches(shuffleId)

    // Remove shuffle files from S3.
    if (dispatcher.cleanupShuffleFiles) {
      // Delete all
      dispatcher.removeShuffle(shuffleId)
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    val cleanupRequired = registeredShuffleIds.nonEmpty
    registeredShuffleIds.foreach(shuffleId => {
      purgeCaches(shuffleId)
      registeredShuffleIds.remove(shuffleId)
    })
    if (cleanupRequired) {
      if (dispatcher.cleanupShuffleFiles) {
        logInfo(f"Cleaning up shuffle files in ${dispatcher.rootDir}.")
        dispatcher.removeRoot()
      } else {
        logInfo(f"Manually cleanup shuffle files in ${dispatcher.rootDir}")
      }
    }
    shuffleBlockResolver.stop()
  }
}

private[spark] object S3ShuffleManager {
  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    if (conf.get("spark.shuffle.sort.io.plugin.class") != "org.apache.spark.shuffle.S3ShuffleDataIO") {
      throw new RuntimeException(
        "\"spark.shuffle.sort.io.plugin.class\" needs to be set to \"org.apache.spark.shuffle.S3ShuffleDataIO\" in order for this plugin to work!"
      )
    }
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap
    executorComponents.initializeExecutor(conf.getAppId, SparkEnv.get.executorId, extraConfigs.asJava)
    executorComponents
  }
}
