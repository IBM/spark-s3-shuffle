package org.apache.spark.shuffle.helper

import org.apache.spark.{SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ConcurrentObjectMap
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockId, ShuffleChecksumBlockId, ShuffleIndexBlockId}

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.util.zip.{Adler32, CRC32, Checksum}

object S3ShuffleHelper extends Logging {
  private lazy val dispatcher = S3ShuffleDispatcher.get

  private val cachedChecksums = new ConcurrentObjectMap[ShuffleChecksumBlockId, Array[Long]]()
  private val cachedArrayLengths = new ConcurrentObjectMap[ShuffleIndexBlockId, Array[Long]]()

  /**
   * Purge cached shuffle indices.
   *
   * @param shuffleIndex
   */
  def purgeCachedDataForShuffle(shuffleIndex: Int): Unit = {
    if (dispatcher.cachePartitionLengths) {
      val filter = (block: ShuffleIndexBlockId) => block.shuffleId == shuffleIndex
      cachedArrayLengths.remove(filter, None)
    }
    if (dispatcher.cacheChecksums) {
      val filter = (block: ShuffleChecksumBlockId) => block.shuffleId == shuffleIndex
      cachedChecksums.remove(filter, None)
    }
  }

  /**
   * Write partitionLengths for block with shuffleId and mapId at 0.
   *
   * @param shuffleId
   * @param mapId
   * @param partitionLengths
   */
  def writePartitionLengths(shuffleId: Int, mapId: Long, partitionLengths: Array[Long]): Unit = {
    val accumulated = Array[Long](0) ++ partitionLengths.tail.scan(partitionLengths.head)(_ + _)
    writeArrayAsBlock(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID), accumulated)
  }

  def writeChecksum(shuffleId: Int, mapId: Long, checksums: Array[Long]): Unit = {
    writeArrayAsBlock(ShuffleChecksumBlockId(shuffleId = shuffleId, mapId = mapId, reduceId = 0), checksums)
  }

  def writeArrayAsBlock(blockId: BlockId, array: Array[Long]): Unit = {
    val file = dispatcher.createBlock(blockId)
    val out = new DataOutputStream(new BufferedOutputStream(file, scala.math.min(8192, 8 * array.length)))
    array.foreach(out.writeLong)
    out.flush()
    out.close()
  }

  /**
   * Get the cached partition length for shuffle index at shuffleId and mapId
   *
   * @param shuffleId
   * @param mapId
   * @return
   */
  def getPartitionLengths(shuffleId: Int, mapId: Long): Array[Long] = {
    getPartitionLengths(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Get the cached partition length for the shuffleIndex block.
   *
   * @param blockId
   * @return
   */
  def getPartitionLengths(blockId: ShuffleIndexBlockId): Array[Long] = {
    if (dispatcher.cachePartitionLengths) {
      return cachedArrayLengths.getOrElsePut(blockId, readBlockAsArray)
    }
    readBlockAsArray(blockId)
  }

  def getChecksums(shuffleId: Int, mapId: Long): Array[Long] = {
    getChecksums(ShuffleChecksumBlockId(shuffleId, mapId, 0))
  }

  def getChecksums(blockId: ShuffleChecksumBlockId): Array[Long] = {
    if (dispatcher.cacheChecksums) {
      return cachedChecksums.getOrElsePut(blockId, readBlockAsArray)
    }
    readBlockAsArray(blockId)
  }

  def createChecksumAlgorithm(algorithm: String): Checksum = {
    algorithm match {
      case "ADLER32" =>
        new Adler32()
      case "CRC32" =>
        new CRC32()
      case _ =>
        throw new UnsupportedOperationException(f"Unsupported shuffle checksum algorithm: ${algorithm}.")
    }
  }

  private def readBlockAsArray(blockId: BlockId): Array[Long] = {
    val stat = dispatcher.getFileStatusCached(blockId)
    val fileLength = stat.getLen.toInt
    val input = new DataInputStream(new BufferedInputStream(dispatcher.openBlock(blockId), math.min(fileLength, dispatcher.bufferSize)))
    val count = fileLength / 8
    if (fileLength % 8 != 0) {
      throw new SparkException(s"Unexpected file length when reading ${blockId.name}")
    }
    val result = new Array[Long](count)
    for (pos <- 0 until count) {
      result(pos) = input.readLong()
    }
    result
  }
}
