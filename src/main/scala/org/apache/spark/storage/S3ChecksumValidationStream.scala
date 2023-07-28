/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.helper.S3ShuffleHelper

import java.io.InputStream
import java.util.zip.Checksum

/**
 * Validates checksum stored for blockId on stream with checksumAlgorithm.
 */
class S3ChecksumValidationStream(
                                  blockId: BlockId,
                                  stream: InputStream,
                                  checksumAlgorithm: String) extends InputStream with Logging {

  private val (shuffleId: Int, mapId: Long, startReduceId: Int, endReduceId: Int) = blockId match {
    case ShuffleBlockId(shuffleId, mapId, reduceId) => (shuffleId, mapId, reduceId, reduceId + 1)
    case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, endReduceId) => (shuffleId, mapId, startReduceId, endReduceId)
    case _ => throw new SparkException(s"S3ChecksumValidationStream does not support block type ${blockId}")
  }

  private val checksum: Checksum = S3ShuffleHelper.createChecksumAlgorithm(checksumAlgorithm)
  private val lengths: Array[Long] = S3ShuffleHelper.getPartitionLengths(shuffleId, mapId)
  private val referenceChecksums: Array[Long] = S3ShuffleHelper.getChecksums(shuffleId, mapId)

  private var pos: Long = 0
  private var reduceId: Int = startReduceId
  private var blockLength: Long = lengths(reduceId)

  private def eof(): Boolean = reduceId > endReduceId

  validateChecksum()

  override def read(): Int = synchronized {
    val res = stream.read()
    if (res > 0) {
      checksum.update(res)
      pos += 1
      validateChecksum()
    }
    if (eof() && res >= 0) {
      throw new SparkException(s"Read ${res} bytes even though we're at end of stream.")
    }
    res
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = synchronized {
    val l: Int = scala.math.min(len, blockLength - pos).toInt
    val res = stream.read(b, off, l)
    if (res > 0) {
      checksum.update(b, off, res)
      pos += res
      validateChecksum()
    }
    if (eof() && res >= 0) {
      logError(s"Read ${res} bytes even though we're at end of stream.")
    }
    res
  }

  private def validateChecksum(): Unit = synchronized {
    if (pos != blockLength) {
      return
    }
    if (checksum.getValue != referenceChecksums(reduceId)) {
      throw new SparkException(s"Invalid checksum detected for ${blockId.name}")
    }
    checksum.reset()
    pos = 0
    reduceId += 1
    if (reduceId < endReduceId) {
      blockLength = lengths(reduceId)
      if (blockLength == 0) {
        validateChecksum()
      }
    } else {
      blockLength = Long.MaxValue
    }
  }

  override def close(): Unit = {
    super.close()
    stream.close()
  }
}
