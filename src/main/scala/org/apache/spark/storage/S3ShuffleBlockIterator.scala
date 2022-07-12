/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import org.apache.spark.shuffle.helper.{S3ShuffleDispatcher, S3ShuffleHelper}

class S3ShuffleBlockIterator(
                              shuffleBlocks: Iterator[BlockId]
                            ) extends Iterator[(BlockId, S3ShuffleBlockStream)] {

  private val dispatcher = S3ShuffleDispatcher.get

  override def hasNext: Boolean = nextValue.isDefined

  override def next(): (BlockId, S3ShuffleBlockStream) = {
    val result = nextValue
    nextValue = computeNext()
    result.get
  }

  private var nextValue: Option[(BlockId, S3ShuffleBlockStream)] = computeNext()

  private def computeNext(): Option[(BlockId, S3ShuffleBlockStream)] = {
    if (!shuffleBlocks.hasNext) {
      return None
    }
    do {
      val nextBlock = shuffleBlocks.next()

      /**
       * Ignore missing index files if `alwaysCreateIndex` is configured.
       */
      try {
        val stream = nextBlock match {
          case ShuffleBlockId(shuffleId, mapId, reduceId) =>
            val lengths = getAccumulatedLengths(shuffleId, mapId)
            new S3ShuffleBlockStream(shuffleId, mapId, reduceId, reduceId + 1, lengths)
          case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, endReduceId) =>
            val lengths = getAccumulatedLengths(shuffleId, mapId)
            new S3ShuffleBlockStream(shuffleId, mapId, startReduceId, endReduceId, lengths)
          case _ => throw new RuntimeException(f"Unexpected block ${nextBlock}.")
        }
        return Some((nextBlock, stream))
      } catch {
        case throwable: Throwable =>
          if (dispatcher.alwaysCreateIndex || dispatcher.useBlockManager) {
            // The index does not exist. This looks like a COS/Consistency bug.
            throw throwable
          }
          // Assume that everything is okay.
      }
    } while (shuffleBlocks.hasNext)
    None
  }

  def getAccumulatedLengths(shuffleId: Int, mapId: Long): Array[Long] = {
    val lengths = S3ShuffleHelper.getPartitionLengthsCached(shuffleId, mapId)
    Array[Long](0) ++ lengths.tail.scan(lengths.head)(_ + _)
  }
}
