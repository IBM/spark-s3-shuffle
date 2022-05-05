/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle.sort

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.storage.ShuffleDataBlockId

import java.io.BufferedOutputStream

/**
 * Implements the ShuffleMapOutputWriter interface. It stores the shuffle output in one
 * shuffle block.
 */
class S3ShuffleMapOutputWriter(
                                shuffleId: Int,
                                mapId: Long,
                                numPartitions: Int,
                                writeMetrics: ShuffleWriteMetricsReporter
                              ) extends ShuffleMapOutputWriter with Logging {

  /* Target block for writing */
  private val shuffleBlock = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
  private val bufferedBlockStream = new BufferedOutputStream(S3ShuffleDispatcher.get.createBlock(shuffleBlock))

  private val partitionLengths = Array.fill[Long](numPartitions)(0)
  private var currentWriter: S3ShufflePartitionWriter = _
  private var lastPartitionWriterId: Int = -1

  /**
   * @param reducePartitionId Monotonically increasing, as per contract in ShuffleMapOutputWriter.
   * @return An instance of the ShufflePartitionWriter exposing the single output stream.
   */
  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    if (reducePartitionId <= lastPartitionWriterId) {
      throw new RuntimeException("Precondition: Expect a monotonically increasing reducePartitionId.")
    }
    if (reducePartitionId >= numPartitions) {
      throw new RuntimeException("Precondition: Invalid partition id.")
    }

    /** Ensure previous writer is closed. */
    if (lastPartitionWriterId >= 0) {
      val numBytes = currentWriter.getNumBytesWritten
      partitionLengths(lastPartitionWriterId) = numBytes
      writeMetrics.incBytesWritten(numBytes)
      currentWriter.close()
    }

    lastPartitionWriterId = reducePartitionId
    currentWriter = new S3ShufflePartitionWriter(bufferedBlockStream)
    currentWriter
  }

  /**
   * Close all writers and the shuffle block.
   *
   * @param checksums Ignored.
   * @return
   */
  override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage = {
    if (checksums.length != numPartitions) {
      throw new RuntimeException("Invalid checksum length.")
    }

    if (lastPartitionWriterId >= 0) {
      val numBytes = currentWriter.getNumBytesWritten
      partitionLengths(lastPartitionWriterId) = numBytes
      writeMetrics.incBytesWritten(numBytes)
      currentWriter.close()
    }
    bufferedBlockStream.close()

    // Write index
    if (partitionLengths.sum > 0 || S3ShuffleDispatcher.get.alwaysCreateIndex) {
      val writeStartTime = System.nanoTime()
      S3ShuffleHelper.writePartitionLengths(shuffleId, mapId, partitionLengths)
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime)
    }
    MapOutputCommitMessage.of(partitionLengths)
  }

  override def abort(error: Throwable): Unit = {
    logError("Abort: [NYI]")
  }
}