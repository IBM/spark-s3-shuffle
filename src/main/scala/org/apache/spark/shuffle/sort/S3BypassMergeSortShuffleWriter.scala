/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle.sort

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, ShuffleDataBlockId}
import org.apache.spark.{SparkEnv, TaskContext}

import java.io.{BufferedInputStream, BufferedOutputStream}
import scala.reflect.ClassTag

class S3BypassMergeSortShuffleWriter[K, V](
                                            blockManager: BlockManager,
                                            handle: BaseShuffleHandle[K, V, _],
                                            mapId: Long,
                                            context: TaskContext,
                                            writeMetrics: ShuffleWriteMetricsReporter,
                                            serializerManager: SerializerManager = SparkEnv.get.serializerManager,
                                          ) extends ShuffleWriter[K, V] with Logging {
  logInfo("Using S3BypassMergeSortShuffleWriter.")
  private lazy val dispatcher = S3ShuffleDispatcher.get
  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val shuffleId = dep.shuffleId
  private val partitioner = dep.partitioner
  private val localFs = FileSystem.getLocal(SparkHadoopUtil.newConfiguration(conf))

  private var stopping = false
  private var mapStatus: MapStatus = null
  private val numPartitions = partitioner.numPartitions
  private val partitionLengths = new Array[Long](numPartitions)

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val serializer = dep.serializer.newInstance()

    val writeStartTime = System.nanoTime()

    class LazyShuffleBlock(reduceId: Int) {
      val blockId = ShuffleBlockId(shuffleId, mapId, reduceId)
      var used: Boolean = false
      private lazy val (_, tempFile) = blockManager.diskBlockManager.createTempShuffleBlock()
      private lazy val tempPath = new Path(tempFile.getPath)
      private lazy val tempWriter = localFs.create(tempPath)
      private lazy val buffer = new BufferedOutputStream(tempWriter)
      private lazy val stream = serializer.serializeStream(serializerManager.wrapStream(blockId, buffer))

      def writeKey[T: ClassTag](key: T): SerializationStream = {
        used = true
        stream.writeKey(key)
      }

      def writeValue[T: ClassTag](value: T): SerializationStream = {
        used = true
        stream.writeValue(value)
      }

      def close(): Unit = {
        if (used) {
          stream.flush()
          stream.close()
          buffer.flush()
          buffer.close()
          tempWriter.flush()
          tempWriter.close()
        }
      }

      def getPath() = {
        if (!used) {
          throw new RuntimeException("Trying to read an unused shuffle block")
        }
        tempPath
      }
    }

    var recordCount: Long = 0
    val shuffleBlocks = Range(0, numPartitions).map(idx => {
      new LazyShuffleBlock(idx)
    })

    {
      val iter = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          dep.aggregator.get.combineValuesByKey(records, context)
        } else {
          records
        }
      } else {
        require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
        records
      }

      for (elem <- iter) {
        val key: K = elem._1
        val value = elem._2
        val partition = shuffleBlocks(partitioner.getPartition(key))
        partition.writeKey(key.asInstanceOf[Object])
        partition.writeValue(value)
        recordCount += 1
      }

      // Flush and close shuffle blocks and its underlying streams.
      shuffleBlocks.foreach(block => {
        block.close()
      })
    }

    // Merge blocks together and store in f"{shuffleId}_${mapId}_0.data"
    if (recordCount > 0) {
      val bufferedWriter = {
        val block = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
        new BufferedOutputStream(dispatcher.createBlock(block))
      }

      // Copy individual blocks into merged block:
      val buffer = new Array[Byte](8192)
      shuffleBlocks.foreach(shuffleBlock => {
        val reduceId = shuffleBlock.blockId.reduceId
        if (shuffleBlock.used) {
          val path = shuffleBlock.getPath()
          val input = localFs.open(shuffleBlock.getPath())
          val bufferedInputStream = new BufferedInputStream(input, buffer.length)
          var count = 0
          var bytesWritten = 0
          do {
            count = bufferedInputStream.read(buffer)
            if (count >= 0) {
              bufferedWriter.write(buffer, 0, count)
              bytesWritten += count
            }
          } while (count >= 0)
          bufferedInputStream.close()
          input.close()

          // Delete temporary file
          localFs.delete(path, false)
          // Update partitionLengths
          partitionLengths(reduceId) = bytesWritten
          writeMetrics.incBytesWritten(bytesWritten)
        } else {
          partitionLengths(reduceId) = 0
        }
      })
      // Flush and close the underlying stream.
      bufferedWriter.flush()
      bufferedWriter.close()
    }
    else {
      Range(0, numPartitions).foreach(idx => {
        partitionLengths(idx) = 0
      })
    }
    writeMetrics.incRecordsWritten(recordCount)

    // Write index
    if (recordCount > 0 || dispatcher.alwaysCreateIndex) {
      S3ShuffleHelper.writePartitionLengths(shuffleId, mapId, partitionLengths)
    }
    writeMetrics.incWriteTime(System.nanoTime() - writeStartTime)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true
    if (success) {
      return Option(mapStatus)
    } else {
      return None
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}
