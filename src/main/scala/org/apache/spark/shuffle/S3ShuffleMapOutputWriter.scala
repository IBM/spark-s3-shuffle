/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle

import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter, WritableByteChannelWrapper}
import org.apache.spark.shuffle.helper.{S3ShuffleDispatcher, S3ShuffleHelper}
import org.apache.spark.storage.ShuffleDataBlockId

import java.io.{BufferedOutputStream, IOException, OutputStream}
import java.nio.channels.{Channels, WritableByteChannel}
import java.util.Optional

/**
 * Implements the ShuffleMapOutputWriter interface. It stores the shuffle output in one
 * shuffle block.
 *
 * This file is based on Spark "LocalDiskShuffleMapOutputWriter.java".
 */

class S3ShuffleMapOutputWriter(
                                conf: SparkConf,
                                shuffleId: Int,
                                mapId: Long,
                                numPartitions: Int,
                              ) extends ShuffleMapOutputWriter with Logging {
  val dispatcher = S3ShuffleDispatcher.get

  /* Target block for writing */
  private val shuffleBlock = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
  private var blockStream: FSDataOutputStream = _
  private var bufferedBlockStream: OutputStream = _
  private var blockStreamAsChannel: WritableByteChannel = _
  private var reduceIdStreamPosition: Long = 0

  def initStream(): Unit = {
    if (blockStream == null) {
      val bufferSize = conf.get(SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE).toInt * 1024
      blockStream = dispatcher.createBlock(shuffleBlock)
      bufferedBlockStream = new BufferedOutputStream(blockStream, bufferSize)
    }
  }

  def initChannel(): Unit = {
    if (blockStreamAsChannel == null) {
      initStream()
      blockStreamAsChannel = Channels.newChannel(bufferedBlockStream)
    }
  }

  private val partitionLengths = Array.fill[Long](numPartitions)(0)
  private var totalBytesWritten: Long = 0
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
    if (bufferedBlockStream != null) {
      bufferedBlockStream.flush()
    }
    if (blockStream != null) {
      blockStream.flush()
      reduceIdStreamPosition = blockStream.getPos
    }
    lastPartitionWriterId = reducePartitionId
    return new S3ShufflePartitionWriter(reducePartitionId)
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

    if (bufferedBlockStream != null) {
      bufferedBlockStream.flush()
    }
    if (blockStream != null) {
      blockStream.flush()
      if (blockStream.getPos != totalBytesWritten) {
        throw new RuntimeException(f"S3ShuffleMapOutputWriter: Unexpected output length ${blockStream.getPos}, expected: ${totalBytesWritten}.")
      }
    }
    if (bufferedBlockStream != null) {
      // Closes the underlying blockstream as well!
      bufferedBlockStream.close()
    }

    // Write index
    if (partitionLengths.sum > 0 || S3ShuffleDispatcher.get.alwaysCreateIndex) {
      S3ShuffleHelper.writePartitionLengths(shuffleId, mapId, partitionLengths)
    }
    MapOutputCommitMessage.of(partitionLengths)
  }

  override def abort(error: Throwable): Unit = {
    cleanUp()
  }

  private def cleanUp(): Unit = {
    if (blockStreamAsChannel != null) {
      blockStreamAsChannel.close()
    }
    if (bufferedBlockStream != null) {
      bufferedBlockStream.close()
    }
    if (blockStream != null) {
      blockStream.close()
    }
  }

  private class S3ShufflePartitionWriter(reduceId: Int) extends ShufflePartitionWriter with Logging {
    private var partitionStream: S3ShuffleOutputStream = _
    private var partitionChannel: S3ShufflePartitionWriterChannel = _

    override def openStream(): OutputStream = {
      initStream()
      if (partitionStream == null) {
        partitionStream = new S3ShuffleOutputStream(reduceId)
      }
      partitionStream
    }

    override def openChannelWrapper(): Optional[WritableByteChannelWrapper] = {
      if (partitionChannel == null) {
        initChannel()
        partitionChannel = new S3ShufflePartitionWriterChannel(reduceId)
      }
      Optional.of(partitionChannel)
    }

    override def getNumBytesWritten: Long = {
      if (partitionChannel != null) {
        return partitionChannel.numBytesWritten
      }
      if (partitionStream != null) {
        return partitionStream.numBytesWritten
      }
      // The partition is empty.
      0
    }
  }

  private class S3ShuffleOutputStream(reduceId: Int) extends OutputStream {
    private var byteCount: Long = 0
    private var isClosed = false

    def numBytesWritten: Long = byteCount

    override def write(b: Int): Unit = {
      if (isClosed) {
        throw new IOException("S3ShuffleOutputStream is already closed.")
      }
      bufferedBlockStream.write(b)
      byteCount += 1
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      if (isClosed) {
        throw new IOException("S3ShuffleOutputStream is already closed.")
      }
      bufferedBlockStream.write(b, off, len)
      byteCount += len
    }

    override def flush(): Unit = {
      if (isClosed) {
        throw new IOException("S3ShuffleOutputStream is already closed.")
      }
      bufferedBlockStream.flush()
    }

    override def close(): Unit = {
      partitionLengths(reduceId) = byteCount
      totalBytesWritten += byteCount
      isClosed = true
    }
  }

  private class S3ShufflePartitionWriterChannel(reduceId: Int) extends WritableByteChannelWrapper {
    private val startPosition: Long = blockStream.getPos

    override def channel(): WritableByteChannel = blockStreamAsChannel

    def numBytesWritten: Long = blockStream.getPos - startPosition

    override def close(): Unit = {
      partitionLengths(reduceId) = numBytesWritten
      bufferedBlockStream.flush()
      blockStream.flush()
      totalBytesWritten += numBytesWritten
    }
  }
}