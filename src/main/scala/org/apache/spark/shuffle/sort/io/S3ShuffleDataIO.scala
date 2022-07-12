/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle.sort.io

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.api.{ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents, ShuffleMapOutputWriter}
import org.apache.spark.shuffle.sort.S3ShuffleDispatcher
import org.apache.spark.storage.BlockManagerMaster

import java.util
import java.util.Collections

class S3ShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  override def executor(): ShuffleExecutorComponents = new S3ShuffleExecutorComponents()

  override def driver(): ShuffleDriverComponents = new S3ShuffleDriverComponents()

  private class S3ShuffleExecutorComponents extends ShuffleExecutorComponents {
    private val blockManager = SparkEnv.get.blockManager
    private val blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager)

    override def initializeExecutor(appId: String, execId: String, extraConfigs: util.Map[String, String]): Unit = {
      // ToDo: Implement dispatcher.
    }

    override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int): ShuffleMapOutputWriter = {
      new S3ShuffleMapOutputWriter(sparkConf, shuffleId, mapTaskId, numPartitions)
    }
  }

  private class S3ShuffleDriverComponents extends ShuffleDriverComponents {
    private var blockManagerMaster: BlockManagerMaster = null
    private var dispatcher: S3ShuffleDispatcher = null

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

    }
  }
}

