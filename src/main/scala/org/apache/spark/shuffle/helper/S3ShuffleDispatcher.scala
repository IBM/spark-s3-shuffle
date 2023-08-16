/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle.helper

import org.apache.hadoop.fs._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.{MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM, SHUFFLE_FILE_BUFFER_SIZE}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.shuffle.ConcurrentObjectMap
import org.apache.spark.storage._
import org.apache.spark.{SparkConf, SparkEnv}

import java.io.IOException
import java.net.URI
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Helper class that configures Hadoop FS.
 */
class S3ShuffleDispatcher extends Logging {
  val executorId: String = SparkEnv.get.executorId
  val conf: SparkConf = SparkEnv.get.conf
  val appId: String = conf.getAppId
  val startTime: String = conf.get("spark.app.startTime")

  // Required
  val rootDir = conf.get("spark.shuffle.s3.rootDir", defaultValue = "sparkS3shuffle")
  private val isCOS = rootDir.startsWith("cos://")
  private val isS3A = rootDir.startsWith("s3a://")

  // Optional
  val bufferSize: Int = conf.getInt("spark.shuffle.s3.bufferSize", defaultValue = conf.get(SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024)
  val bufferInputSize: Int = conf.getInt("spark.shuffle.s3.bufferInputSize", defaultValue = conf.get(MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM).toInt)
  val cleanupShuffleFiles: Boolean = conf.getBoolean("spark.shuffle.s3.cleanup", defaultValue = true)
  val folderPrefixes: Int = conf.getInt("spark.shuffle.s3.folderPrefixes", defaultValue = 10)
  val prefetchBatchSize: Int = conf.getInt("spark.shuffle.s3.prefetchBatchSize", defaultValue = 25)
  val prefetchThreadPoolSize: Int = conf.getInt("spark.shuffle.s3.prefetchThreadPoolSize", defaultValue = 100)

  // Debug
  val alwaysCreateIndex: Boolean = conf.getBoolean("spark.shuffle.s3.alwaysCreateIndex", defaultValue = false)
  val useBlockManager: Boolean = conf.getBoolean("spark.shuffle.s3.useBlockManager", defaultValue = true)
  val forceBatchFetch: Boolean = conf.getBoolean("spark.shuffle.s3.forceBatchFetch", defaultValue = false)

  // Backports
  val checksumAlgorithm: String = conf.get("spark.shuffle.checksum.algorithm", defaultValue = "ADLER32")
  val checksumEnabled: Boolean = conf.getBoolean("spark.shuffle.checksum.enabled", defaultValue = false)

  val appDir = f"/${startTime}-${appId}/"
  val fs: FileSystem = FileSystem.get(URI.create(rootDir), {
    SparkHadoopUtil.newConfiguration(conf)
  })

  // Required
  logInfo(s"- spark.shuffle.s3.rootDir=${rootDir} (app dir: ${appDir})")

  // Optional
  logInfo(s"- spark.shuffle.s3.bufferSize=${bufferSize}")
  logInfo(s"- spark.shuffle.s3.bufferInputSize=${bufferInputSize}")
  logInfo(s"- spark.shuffle.s3.cleanup=${cleanupShuffleFiles}")
  logInfo(s"- spark.shuffle.s3.folderPrefixes=${folderPrefixes}")
  logInfo(s"- spark.shuffle.s3.prefetchBlockSize=${prefetchBatchSize}")
  logInfo(s"- spark.shuffle.s3.prefetchThreadPoolSize=${prefetchThreadPoolSize}")

  // Debug
  logInfo(s"- spark.shuffle.s3.alwaysCreateIndex=${alwaysCreateIndex} (default: false)")
  logInfo(s"- spark.shuffle.s3.useBlockManager=${useBlockManager} (default: true)")
  logInfo(s"- spark.shuffle.s3.forceBatchFetch=${forceBatchFetch} (default: false)")

  // Backports
  logInfo(s"- spark.shuffle.checksum.algorithm=${checksumAlgorithm} (backported from Spark 3.2.0)")
  logInfo(s"- spark.shuffle.checksum.enabled=${checksumEnabled} (backported from Spark 3.2.0)")

  def removeRoot(): Boolean = {
    Range(0, folderPrefixes).map(idx => {
      Future {
        val prefix = f"${rootDir}/${idx}${appDir}"
        try {
          fs.delete(new Path(prefix), true)
        } catch {
          case _: IOException => logDebug(s"Unable to delete prefix ${prefix}")
        }
      }
    }).map(Await.result(_, Duration.Inf))
    true
  }

  def getPath(blockId: BlockId): Path = {
    val idx = (blockId match {
      case ShuffleBlockId(_, mapId, _) =>
        mapId
      case ShuffleDataBlockId(_, mapId, _) =>
        mapId
      case ShuffleIndexBlockId(_, mapId, _) =>
        mapId
      case _ => 0
    }) % folderPrefixes
    new Path(f"${rootDir}/${idx}${appDir}/${blockId.name}")
  }

  def getPath(blockId: ShuffleChecksumBlockId): Path = {
    val idx = blockId.mapId % folderPrefixes
    new Path(f"${rootDir}/${idx}${appDir}/${blockId.name}")
  }

  /**
   * Open a block for reading.
   *
   * @param blockId
   * @return
   */
  def openBlock(blockId: BlockId): FSDataInputStream = {
    fs.open(getPath(blockId))
  }

  def openBlock(blockId: ShuffleChecksumBlockId): FSDataInputStream = {
    fs.open(getPath(blockId))
  }

  private val cachedInputStreams = new ConcurrentObjectMap[BlockId, FSDataInputStream]()

  def closeCachedBlocks(shuffleIndex: Int): Unit = {
    val filter = (blockId: BlockId) => blockId match {
      case RDDBlockId(_, _) => false
      case ShuffleBlockId(shuffleId, _, _) => shuffleId == shuffleIndex
      case ShuffleBlockBatchId(shuffleId, _, _, _) => shuffleId == shuffleIndex
      case ShuffleDataBlockId(shuffleId, _, _) => shuffleId == shuffleIndex
      case ShuffleIndexBlockId(shuffleId, _, _) => shuffleId == shuffleIndex
      case BroadcastBlockId(_, _) => false
      case TaskResultBlockId(_) => false
      case StreamBlockId(_, _) => false
      case TempLocalBlockId(_) => false
      case TempShuffleBlockId(_) => false
      case TestBlockId(_) => false
    }
    val action = (stream: FSDataInputStream) => stream.close()
    cachedInputStreams.remove(filter, Option(action))
  }

  /**
   * Open a block for writing.
   *
   * @param blockId
   * @return
   */
  def createBlock(blockId: BlockId): FSDataOutputStream = {
    fs.create(getPath(blockId))
  }

  def createBlock(blockId: ShuffleChecksumBlockId): FSDataOutputStream = {
    fs.create(getPath(blockId))
  }
}

object S3ShuffleDispatcher extends Logging {
  private val lock = new Object()
  private var store: S3ShuffleDispatcher = null

  def get: S3ShuffleDispatcher = {
    if (store == null) {
      lock.synchronized({
        if (store == null) {
          store = new S3ShuffleDispatcher()
        }
      })
    }
    store
  }
}
