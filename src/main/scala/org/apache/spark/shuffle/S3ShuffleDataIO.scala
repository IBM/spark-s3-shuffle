/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle

import org.apache.spark.shuffle.api.{
  ShuffleDataIO,
  ShuffleDriverComponents,
  ShuffleExecutorComponents,
  ShuffleMapOutputWriter,
  SingleSpillShuffleMapOutputWriter
}
import org.apache.spark.shuffle.helper.S3ShuffleDispatcher
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.{SparkConf, SparkEnv}

import java.util
import java.util.{Collections, Optional}

class S3ShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  override def executor(): ShuffleExecutorComponents = new S3ShuffleExecutorComponents()

  override def driver(): ShuffleDriverComponents = new S3ShuffleDriverComponents()

  private class S3ShuffleExecutorComponents extends ShuffleExecutorComponents {
    private val blockManager = SparkEnv.get.blockManager

    override def initializeExecutor(appId: String, execId: String, extraConfigs: util.Map[String, String]): Unit = {
      // ToDo: Implement dispatcher.
    }

    override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int): ShuffleMapOutputWriter = {
      new S3ShuffleMapOutputWriter(sparkConf, shuffleId, mapTaskId, numPartitions)
    }

    override def createSingleFileMapOutputWriter(
                                                  shuffleId: Int,
                                                  mapId: Long
                                                ): Optional[SingleSpillShuffleMapOutputWriter] = {
      // Checksums are not supported for the single spill output writer.
      if (S3ShuffleDispatcher.get.checksumEnabled) {
        return Optional.empty()
      }
      Optional.of(new S3SingleSpillShuffleMapOutputWriter(shuffleId, mapId))
    }
  }

  private class S3ShuffleDriverComponents extends ShuffleDriverComponents {
    private var blockManagerMaster: BlockManagerMaster = null

    override def initializeApplication(): util.Map[String, String] = {
      blockManagerMaster = SparkEnv.get.blockManager.master
      Collections.emptyMap()
    }

    override def cleanupApplication(): Unit = {
      // Dispatcher cleanup
      if (S3ShuffleDispatcher.get.cleanupShuffleFiles) {
        S3ShuffleDispatcher.get.removeRoot()
      }
    }

    override def registerShuffle(shuffleId: Int): Unit = {
      super.registerShuffle(shuffleId)
    }

    override def removeShuffle(shuffleId: Int, blocking: Boolean): Unit = {
      super.removeShuffle(shuffleId, blocking)
    }
  }
}

