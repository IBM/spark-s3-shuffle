package org.apache.spark.shuffle.helper

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ConcurrentObjectMap
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockId, ShuffleChecksumBlockId, ShuffleIndexBlockId}

import java.io.{BufferedInputStream, BufferedOutputStream, IOException}
import java.nio.ByteBuffer
import java.util
import java.util.zip.{Adler32, CRC32, Checksum}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object S3ShuffleHelper extends Logging {
  private lazy val serializer = SparkEnv.get.serializer
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
    writeArrayAsBlock(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID), partitionLengths)
  }

  def writeChecksum(shuffleId: Int, mapId: Long, checksums: Array[Long]): Unit = {
    writeArrayAsBlock(ShuffleChecksumBlockId(shuffleId = shuffleId, mapId = mapId, reduceId = 0), checksums)
  }

  def writeArrayAsBlock(blockId: BlockId, array: Array[Long]): Unit = {
    val serializerInstance = serializer.newInstance()
    val buffer = serializerInstance.serialize[Array[Long]](array)
    val file = dispatcher.createBlock(blockId)
    file.write(buffer.array(), buffer.arrayOffset(), buffer.limit())
    file.flush()
    file.close()
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

  private def readBlockAsArray(blockId: BlockId) = {
    val stat = dispatcher.getFileStatusCached(blockId)
    val fsize = scala.math.min(stat.getLen.toInt, dispatcher.bufferSize)
    val file = new BufferedInputStream(dispatcher.openBlock(blockId), fsize)
    var buffer = new Array[Byte](fsize)
    var numBytes = 0
    var done = false
    while (!done) {
      val c = file.read(buffer, numBytes, buffer.length - numBytes)
      if (c >= 0) {
        numBytes += c
        if (numBytes >= buffer.length) {
          buffer = util.Arrays.copyOf(buffer, buffer.length * 2)
        }
      } else {
        done = true
      }
    }
    val serializerInstance = serializer.newInstance()
    try {
      val result = serializerInstance.deserialize[Array[Long]](ByteBuffer.wrap(buffer, 0, numBytes))
      result
    } catch {
      case e: Exception =>
        logError(e.getMessage)
        throw e
    } finally {
      file.close()
    }
  }
}
