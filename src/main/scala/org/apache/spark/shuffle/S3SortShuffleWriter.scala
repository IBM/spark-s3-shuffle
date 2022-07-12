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

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.sort.S3ShuffleDispatcher
import org.apache.spark.shuffle.sort.io.S3ShuffleMapOutputWriter
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.collection.ExternalSorter

/*
 * This class was adapted from Apache Spark: SortShuffleWriter.scala.
 */
class S3SortShuffleWriter[K, V, C](
                                    conf: SparkConf,
                                    blockManager: BlockManager,
                                    handle: BaseShuffleHandle[K, V, C],
                                    mapId: Long,
                                    context: TaskContext,
                                    writeMetrics: ShuffleWriteMetricsReporter
                                  ) extends ShuffleWriter[K, V] with Logging {
  logInfo("Using S3SortShuffleWriter")
  private val dep = handle.dependency
  private val shuffleId = dep.shuffleId
  private val partitioner = dep.partitioner

  private var stopping = false
  private var mapStatus: MapStatus = null
  private var sorter: ExternalSorter[K, V, _] = null

  private val numPartitions = partitioner.numPartitions
  private var partitionLengths: Array[Long] = _

  private val dispatcher = S3ShuffleDispatcher.get

  override def write(records: Iterator[Product2[K, V]]): Unit = {

    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }

    val copyRecords = if (dispatcher.sortShuffleCloneRecords) {
      records.map {
        record => {
          val key: K = record._1 match {
            case c: Array[_] => c.clone().asInstanceOf[K]
            case _ => record._1
          }
          val value: V = record._2 match {
            case c: Array[_] => c.clone().asInstanceOf[V]
            case _ => record._2
          }
          (key, value)
        }
      }
    } else records

    sorter.insertAll(copyRecords)

    val mapOutputWriter = new S3ShuffleMapOutputWriter(conf, shuffleId, mapId, numPartitions)
    sorter.writePartitionedMapOutput(shuffleId, mapId, mapOutputWriter)

    val writeStartTime = System.nanoTime()
    partitionLengths = mapOutputWriter.commitAllPartitions(sorter.getChecksums).getPartitionLengths
    writeMetrics.incWriteTime(System.nanoTime() - writeStartTime)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime() - startTime)
        sorter = null
      }
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}
