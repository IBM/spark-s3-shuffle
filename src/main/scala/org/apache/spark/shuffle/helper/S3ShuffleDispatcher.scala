/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle.helper

import org.apache.hadoop.fs._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ConcurrentObjectMap
import org.apache.spark.storage._
import org.apache.spark.{SparkConf, SparkEnv}

import java.net.URI

/**
 * Helper class that configures Hadoop FS.
 */
class S3ShuffleDispatcher extends Logging {
  val executorId: String = SparkEnv.get.executorId
  val conf: SparkConf = SparkEnv.get.conf
  val appId: String = conf.getAppId
  val startTime: String = conf.get("spark.app.startTime")

  val cleanupShuffleFiles: Boolean = conf.getBoolean("spark.shuffle.s3.cleanup", defaultValue = true)
  private val rootDir_ = conf.get("spark.shuffle.s3.rootDir", defaultValue = "sparkS3shuffle")
  private val isCOS = rootDir_.startsWith("cos://")
  private val isS3A = rootDir_.startsWith("s3a://")
  val supportsUnbuffer: Boolean = conf.getBoolean("spark.shuffle.s3.supportsUnbuffer", defaultValue = isS3A)
  val alwaysCreateIndex: Boolean = conf.getBoolean("spark.shuffle.s3.alwaysCreateIndex", defaultValue = false)
  val useBlockManager: Boolean = conf.getBoolean("spark.shuffle.s3.useBlockManager", defaultValue = true)
  val forceBatchFetch: Boolean = conf.getBoolean("spark.shuffle.s3.forceBatchFetch", defaultValue = false)
  val allowSerializedShuffle: Boolean = conf.getBoolean("spark.shuffle.s3.allowSerializedShuffle", defaultValue = true)
  val forceBypassMergeSort: Boolean = conf.getBoolean("spark.shuffle.s3.forceBypassMergeSort", defaultValue = false)
  val sortShuffleCloneRecords: Boolean = conf.getBoolean("spark.shuffle.s3.sort.cloneRecords", defaultValue = false)

  val rootDir: String = f"${rootDir_}/${startTime}-${appId}/"
  val fs: FileSystem = FileSystem.get(URI.create(rootDir), {
    SparkHadoopUtil.newConfiguration(conf)
  })

  logInfo(s"- spark.shuffle.s3.rootDir=${rootDir_} (run specific: ${rootDir})")
  logInfo(s"- spark.shuffle.s3.cleanup=${cleanupShuffleFiles}")
  logInfo(s"- spark.shuffle.s3.supportsUnbuffer=${supportsUnbuffer}")
  logInfo(s"- spark.shuffle.s3.alwaysCreateIndex=${alwaysCreateIndex}")
  logInfo(s"- spark.shuffle.s3.useBlockManager=${useBlockManager}")
  logInfo(s"- spark.shuffle.s3.forceBatchFetch=${forceBatchFetch}")
  logInfo(s"- spark.shuffle.s3.allowSerializedShuffle=${allowSerializedShuffle}")
  logInfo(s"- spark.shuffle.s3.forceBypassMergeSort=${forceBypassMergeSort}")
  logInfo(s"- spark.shuffle.s3.sort.cloneRecords=${sortShuffleCloneRecords}")

  def removeRoot(): Boolean = {
    fs.delete(new Path(rootDir), true)
  }

  def getPath(blockId: BlockId): Path = {
    new Path(f"${rootDir}/${blockId.name}")
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

  private val cachedInputStreams = new ConcurrentObjectMap[BlockId, FSDataInputStream]()

  /**
   * Reuse an already opened input stream. Assumes that the data stream supports unbuffering.
   *
   * @param blockId
   * @return
   */
  def openCachedBlock(blockId: BlockId): FSDataInputStream = {
    cachedInputStreams.getOrElsePut(blockId, openBlock)
  }

  def closeCachedBlocks(shuffleIndex: Int): Unit = {
    val filter = (blockId: BlockId) => blockId match {
      case RDDBlockId(_, _) => false
      case ShuffleBlockId(shuffleId, _, _) => shuffleId == shuffleIndex
      case ShuffleBlockBatchId(shuffleId, _, _, _) => shuffleId == shuffleIndex
      case ShuffleBlockChunkId(shuffleId, _, _, _) => shuffleId == shuffleIndex
      case ShuffleDataBlockId(shuffleId, _, _) => shuffleId == shuffleIndex
      case ShuffleIndexBlockId(shuffleId, _, _) => shuffleId == shuffleIndex
      case ShuffleChecksumBlockId(shuffleId, _, _) => shuffleId == shuffleIndex
      case ShufflePushBlockId(shuffleId, _, _, _) => shuffleId == shuffleIndex
      case ShuffleMergedBlockId(shuffleId, _, _) => shuffleId == shuffleIndex
      case ShuffleMergedDataBlockId(_, shuffleId, _, _) => shuffleId == shuffleIndex
      case ShuffleMergedIndexBlockId(_, shuffleId, _, _) => shuffleId == shuffleIndex
      case ShuffleMergedMetaBlockId(_, shuffleId, _, _) => shuffleId == shuffleIndex
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
