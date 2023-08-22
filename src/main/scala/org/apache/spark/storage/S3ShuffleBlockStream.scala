/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.helper.S3ShuffleDispatcher

import java.io.{EOFException, IOException, InputStream}

/**
 * InputStream that reads data from a shuffleBlock, mapId and exposes an InputStream from startReduceId to endReduceId.
 */
class S3ShuffleBlockStream(
                            shuffleId: Int,
                            mapId: Long,
                            startReduceId: Int,
                            endReduceId: Int,
                            accumulatedPositions: Array[Long],
                          ) extends InputStream with Logging {
  private lazy val dispatcher = S3ShuffleDispatcher.get
  private lazy val blockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
  private lazy val stream = {
    try {
      dispatcher.openBlock(blockId)
    } catch {
      case throwable: Throwable =>
        logError(f"Unable to open block ${blockId.name}")
        throw throwable
    }
  }

  private val startPosition = accumulatedPositions(startReduceId)
  private val endPosition = accumulatedPositions(endReduceId)
  private var streamClosed = startPosition == endPosition // automatically mark stream as closed if length is empty.

  val maxBytes: Long = endPosition - startPosition
  private var numBytes: Long = 0

  private val singleByteBuffer = new Array[Byte](1)

  override def close(): Unit = synchronized {
    if (streamClosed) {
      return
    }
    this.synchronized {
      stream.close()
      streamClosed = true
    }
    super.close()
  }

  override def read(): Int = synchronized {
    if (streamClosed || numBytes >= maxBytes) {
      return -1
    }
    try {
      stream.readFully(startPosition + numBytes, singleByteBuffer)
      numBytes += 1
      if (numBytes >= maxBytes) {
        close()
      }
      return singleByteBuffer(0)
    } catch {
      case e: IOException =>
        logError(f"Encountered an unexpected IOException: ${e.toString}")
        close()
        return -1
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = synchronized {
    if (streamClosed || numBytes >= maxBytes) {
      return -1
    }
    val maxLength = (maxBytes - numBytes).toInt
    assert(maxLength >= 0)
    val length = math.min(maxLength, len)
    try {
      stream.readFully(startPosition + numBytes, b, off, length)
      numBytes += length
      if (numBytes >= maxBytes) {
        close()
      }
      return length
    } catch {
      case e: IOException =>
        logError(f"Encountered an unexpected IOException: ${e.toString}")
        close()
        return -1
    }
  }

  override def skip(n: Long): Long = synchronized {
    if (streamClosed || numBytes >= maxBytes || n <= 0) {
      return 0
    }
    val toSkip = math.min(maxBytes - numBytes, n)
    val skipped = stream.skip(toSkip)
    numBytes += skipped
    skipped
  }

  override def available(): Int = synchronized {
    if (streamClosed) {
      return 0
    }
    return (maxBytes - numBytes).toInt
  }
}
