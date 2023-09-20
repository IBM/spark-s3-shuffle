/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter
import org.apache.spark.shuffle.helper.{S3ShuffleDispatcher, S3ShuffleHelper}
import org.apache.spark.storage.ShuffleDataBlockId
import org.apache.spark.util.Utils

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path}

class S3SingleSpillShuffleMapOutputWriter(shuffleId: Int, mapId: Long) extends SingleSpillShuffleMapOutputWriter with Logging {

  private lazy val dispatcher = S3ShuffleDispatcher.get

  override def transferMapSpillFile(
                                     mapSpillFile: File,
                                     partitionLengths: Array[Long],
                                     checksums: Array[Long]
                                   ): Unit = {
    val block = ShuffleDataBlockId(shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)

    if (dispatcher.rootIsLocal) {
      // Use NIO to move the file if the folder is local.
      val now = System.nanoTime()
      val path = dispatcher.getPath(block)
      val fileDestination = path.toUri.getRawPath
      val dir = path.getParent
      if (!dispatcher.fs.exists(dir)) {
        dispatcher.fs.mkdirs(dir)
      }
      Files.move(mapSpillFile.toPath, Path.of(fileDestination))
      val timings = System.nanoTime() - now

      val bytes = partitionLengths.sum
      val tc = TaskContext.get()
      val sId = tc.stageId()
      val sAt = tc.stageAttemptNumber()
      val t = timings / 1000000
      val bw = bytes.toDouble / (t.toDouble / 1000) / (1024 * 1024)
      logInfo(s"Statistics: Stage ${sId}.${sAt} TID ${tc.taskAttemptId()} -- " +
                s"Writing ${block.name} ${bytes} took ${t} ms (${bw} MiB/s)")
    } else {
      // Copy using a stream.
      val in = new FileInputStream(mapSpillFile)
      val out = new S3MeasureOutputStream(dispatcher.createBlock(block), block.name)
      Utils.copyStream(in, out, closeStreams = true)
    }

    if (dispatcher.checksumEnabled) {
      S3ShuffleHelper.writeChecksum(shuffleId, mapId, checksums)
    }
    S3ShuffleHelper.writePartitionLengths(shuffleId, mapId, partitionLengths)
  }
}
