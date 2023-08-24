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

package org.apache.spark.storage

import org.apache.spark.internal.{Logging, config}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.helper.{S3ShuffleDispatcher, S3ShuffleHelper}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReadMetricsReporter, ShuffleReader}
import org.apache.spark.storage.ShuffleBlockFetcherIterator.FetchBlockInfo
import org.apache.spark.util.{CompletionIterator, ThreadUtils}
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, SparkConf, SparkEnv, TaskContext}

import scala.concurrent.{ExecutionContext}

/**
 * This class was adapted from Apache Spark: BlockStoreShuffleReader.
 */
class S3ShuffleReader[K, C](
                             conf: SparkConf,
                             handle: BaseShuffleHandle[K, _, C],
                             context: TaskContext,
                             readMetrics: ShuffleReadMetricsReporter,
                             startMapIndex: Int,
                             endMapIndex: Int,
                             startPartition: Int,
                             endPartition: Int,
                             serializerManager: SerializerManager = SparkEnv.get.serializerManager,
                             shouldBatchFetch: Boolean
                           ) extends ShuffleReader[K, C] with Logging {

  private val dispatcher = S3ShuffleDispatcher.get
  private val dep = handle.dependency

  private val fetchContinousBlocksInBatch: Boolean = {
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val ioEncryption = conf.get(config.IO_ENCRYPTION_ENABLED)
    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !ioEncryption
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
                 "we can not enable the feature because other conditions are not satisfied. " +
                 s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
                 s"codec concatenation: $codecConcatenation, io encryption: $ioEncryption.")
    }
    doBatchFetch
  }

  override def read(): Iterator[Product2[K, C]] = {
    val serializerInstance = dep.serializer.newInstance()
    val blocks = computeShuffleBlocks(handle.shuffleId,
                                      startMapIndex, endMapIndex,
                                      startPartition, endPartition,
                                      doBatchFetch = fetchContinousBlocksInBatch,
                                      useBlockManager = dispatcher.useBlockManager)

    val wrappedStreams = new S3ShuffleBlockIterator(blocks)
    val bufferSize = dispatcher.bufferSize.toInt

    // Create a key/value iterator for each stream
    val streamIter = wrappedStreams.filterNot(_._2.maxBytes == 0).map { case (blockId, wrappedStream) =>
      readMetrics.incRemoteBytesRead(wrappedStream.maxBytes) // increase byte count.
      readMetrics.incRemoteBlocksFetched(1)
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.

      val stream = new S3DoubleBufferedStream(wrappedStream, bufferSize)
      val checkedStream = if (dispatcher.checksumEnabled) {
        new S3ChecksumValidationStream(blockId, stream, dispatcher.checksumAlgorithm)
      } else {
        stream
      }

      (blockId, checkedStream)
    }

    val recordIter = new PrefetchIterator(streamIter).flatMap { case (blockId, stream) =>
      serializerInstance
        .deserializeStream(serializerManager.wrapStream(blockId, stream))
        .asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAllAndUpdateMetrics(aggregatedIter)
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  private def computeShuffleBlocks(
                                    shuffleId: Int,
                                    startMapIndex: Int,
                                    endMapIndex: Int,
                                    startPartition: Int,
                                    endPartition: Int,
                                    doBatchFetch: Boolean,
                                    useBlockManager: Boolean
                                  ): Iterator[BlockId] = {
    if (useBlockManager) {
      val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
        handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
      blocksByAddress.map(f => f._2.map(info => FetchBlockInfo(info._1, info._2, info._3)))
                     .flatMap(info => ShuffleBlockFetcherIterator.mergeContinuousShuffleBlockIdsIfNeeded(info, doBatchFetch))
                     .map(_.blockId)
    } else {
      val indices = S3ShuffleHelper.listShuffleIndices(shuffleId).filter(
        block => block.mapId >= startMapIndex && block.mapId < endMapIndex)
      if (doBatchFetch || dispatcher.forceBatchFetch) {
        indices.map(block => ShuffleBlockBatchId(block.shuffleId, block.mapId, startPartition, endPartition)).toIterator
      } else {
        indices.flatMap(block => Range(startPartition, endPartition).map(partition => ShuffleBlockId(block.shuffleId, block.mapId, partition))).toIterator
      }
    }
  }
}
