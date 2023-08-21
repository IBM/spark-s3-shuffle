/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle

import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter
import org.apache.spark.shuffle.helper.{S3ShuffleDispatcher, S3ShuffleHelper}
import org.apache.spark.storage.ShuffleDataBlockId
import org.apache.spark.util.Utils

import java.io.{BufferedOutputStream, File, FileInputStream}

class S3SingleSpillShuffleMapOutputWriter(shuffleId: Int, mapId: Long) extends SingleSpillShuffleMapOutputWriter {

  private lazy val dispatcher = S3ShuffleDispatcher.get

  override def transferMapSpillFile(
                                     mapSpillFile: File,
                                     partitionLengths: Array[Long],
                                     checksums: Array[Long]
                                   ): Unit = {
    val in = new FileInputStream(mapSpillFile)
    val out = dispatcher.createBlock(ShuffleDataBlockId(shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID))

    // Note: HDFS does not exposed a nio-buffered write interface.
    Utils.copyStream(in, out, closeStreams = true)

    if (dispatcher.checksumEnabled) {
      S3ShuffleHelper.writeChecksum(shuffleId, mapId, checksums)
    }
    S3ShuffleHelper.writePartitionLengths(shuffleId, mapId, partitionLengths)
  }
}
