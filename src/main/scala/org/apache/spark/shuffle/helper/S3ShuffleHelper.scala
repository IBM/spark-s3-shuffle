package org.apache.spark.shuffle.helper

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ConcurrentObjectMap
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockId, ShuffleIndexBlockId}

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.nio.ByteBuffer
import java.util
import java.util.regex.Pattern

object S3ShuffleHelper extends Logging {
  private lazy val serializer = SparkEnv.get.serializer
  private lazy val dispatcher = S3ShuffleDispatcher.get

  private val cachedArrayLengths = new ConcurrentObjectMap[ShuffleIndexBlockId, Array[Long]]()
  private val cachedIndexBlocks = new ConcurrentObjectMap[Int, Array[ShuffleIndexBlockId]]()

  /**
   * Purge cached shuffle indices.
   *
   * @param shuffleIndex
   */
  def purgeCachedShuffleIndices(shuffleIndex: Int): Unit = {
    val indexFilter = (idx: Int) => idx == shuffleIndex
    val blockFilter = (block: ShuffleIndexBlockId) => block.shuffleId == shuffleIndex
    cachedIndexBlocks.remove(indexFilter, None)
    cachedArrayLengths.remove(blockFilter, None)
  }

  /**
   * Write partitionLengths for block with shuffleId and mapId at 0.
   *
   * @param shuffleId
   * @param mapId
   * @param partitionLengths
   */
  def writePartitionLengths(shuffleId: Int, mapId: Long, partitionLengths: Array[Long]): Unit = {
    writePartitionLengths(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID), partitionLengths)
  }

  /**
   * Write partitionLengths for blockId.
   *
   * @param blockId
   * @param partitionLengths
   */
  def writePartitionLengths(blockId: ShuffleIndexBlockId, partitionLengths: Array[Long]): Unit = {
    val serializerInstance = serializer.newInstance()
    val buffer = serializerInstance.serialize[Array[Long]](partitionLengths)
    val file = new BufferedOutputStream(dispatcher.createBlock(blockId))
    file.write(buffer.array(), buffer.arrayOffset(), buffer.limit())
    file.flush()
    file.close()
  }

  /**
   * List cached shuffle indices.
   *
   * @param shuffleId
   * @return
   */
  def listShuffleIndicesCached(shuffleId: Int): Array[ShuffleIndexBlockId] = {
    cachedIndexBlocks.getOrElsePut(shuffleId, listShuffleIndices)
  }

  private def listShuffleIndices(shuffleId: Int): Array[ShuffleIndexBlockId] = {
    val shuffleIndexFilter: PathFilter = new PathFilter() {
      private val regex = Pattern.compile(f"shuffle_${shuffleId}" + "_([0-9]+)_([0-9]+).index")

      override def accept(path: Path): Boolean = {
        regex.matcher(path.getName).matches()
      }
    }
    Range(0, 10).flatMap(idx => {
      val path = new Path(f"${dispatcher.rootDir}/${idx}${dispatcher.appDir}")
      dispatcher.fs.listStatus(path, shuffleIndexFilter).map(v => {
        BlockId.apply(v.getPath.getName).asInstanceOf[ShuffleIndexBlockId]
      })
    }).toArray
  }

  /**
   * Get the cached partition length for shuffle index at shuffleId and mapId
   *
   * @param shuffleId
   * @param mapId
   * @return
   */
  def getPartitionLengthsCached(shuffleId: Int, mapId: Long): Array[Long] = {
    getPartitionLengthsCached(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Get the cached partition length for the shuffleIndex block.
   *
   * @param blockId
   * @return
   */
  def getPartitionLengthsCached(blockId: ShuffleIndexBlockId): Array[Long] = {
    cachedArrayLengths.getOrElsePut(blockId, getPartitionLengths)
  }

  private def getPartitionLengths(blockId: ShuffleIndexBlockId): Array[Long] = {
    val file = new BufferedInputStream(dispatcher.openBlock(blockId))
    var buffer = new Array[Byte](1024)
    var numBytes = 0
    var done = false
    do {
      val c = file.read(buffer, numBytes, buffer.length - numBytes)
      if (c >= 0) {
        numBytes += c
        if (numBytes >= buffer.length) {
          buffer = util.Arrays.copyOf(buffer, buffer.length * 2)
        }
      } else {
        done = true
      }
    } while (!done)
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
