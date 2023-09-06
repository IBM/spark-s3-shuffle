/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.helper.S3ShuffleDispatcher

import java.io.{BufferedInputStream, InputStream}
import java.util

class S3BufferedPrefetchIterator(iter: Iterator[(BlockId, S3ShuffleBlockStream)], maxBufferSize: Long) extends Iterator[(BlockId, InputStream)] with Logging {

  private val concurrencyTask = S3ShuffleDispatcher.get.prefetchConcurrencyTask
  private val startTime = System.nanoTime()

  @volatile private var memoryUsage: Long = 0
  @volatile private var hasItem: Boolean = iter.hasNext
  private var timeWaiting: Long = 0
  private var timePrefetching: Long = 0
  private var numStreams: Long = 0
  private var bytesRead: Long = 0

  private var activeTasks: Long = 0

  private val completed = new util.LinkedList[(InputStream, BlockId, Long)]()

  private def prefetchThread(): Unit = {
    var nextElement: (BlockId, S3ShuffleBlockStream) = null
    while (true) {
      synchronized {
        if (!iter.hasNext && nextElement == null) {
          hasItem = false
          return
        }
        if (nextElement == null) {
          nextElement = iter.next()
          activeTasks += 1
          hasItem = iter.hasNext
        }
      }

      var fetchNext = false
      val bsize = scala.math.min(maxBufferSize, nextElement._2.maxBytes).toInt
      synchronized {
        if (memoryUsage + bsize > maxBufferSize) {
          try {
            wait()
          }
          catch {
            case _: InterruptedException =>
          }
        } else {
          fetchNext = true
          memoryUsage += bsize
        }
      }

      if (fetchNext) {
        val block = nextElement._1
        val s = nextElement._2
        nextElement = null
        val now = System.nanoTime()
        val stream = new BufferedInputStream(s, bsize)
        // Fill the buffered input stream by reading and then resetting the stream.
        stream.mark(bsize)
        stream.read()
        stream.reset()
        timePrefetching += System.nanoTime() - now
        bytesRead += bsize
        synchronized {
          completed.push((stream, block, bsize))
          activeTasks -= 1
          notifyAll()
        }
      }
    }
  }

  private val self = this
  private val threads = Array.fill[Thread](concurrencyTask)(new Thread {
    override def run(): Unit = {
      self.prefetchThread()
    }
  })
  threads.foreach(_.start())

  private def printStatistics(): Unit = synchronized {
    val totalRuntime = System.nanoTime() - startTime
    try {
      val tR = totalRuntime / 1000000
      val wPer = 100 * timeWaiting / totalRuntime
      val tW = timeWaiting / 1000000
      val tP = timePrefetching / 1000000
      val bR = bytesRead
      val r = numStreams
      // Average time per prefetch
      val atP = tP / r
      // Average time waiting
      val atW = tW / r
      // Average read bandwidth
      val bW = bR.toDouble / (tP.toDouble / 1000) / (1024 * 1024)
      // Block size
      val bs = bR / r
      logInfo(s"Statistics: ${bR} bytes, ${tW} ms waiting (${atW} avg), " +
                s"${tP} ms prefetching (avg: ${atP} ms - ${bs} block size - ${bW} MiB/s). " +
                s"Total: ${tR} ms - ${wPer}% waiting")
    } catch {
      case e: Exception => logError(f"Unable to print statistics: ${e.getMessage}.")
    }
  }

  override def hasNext: Boolean = synchronized {
    val result = hasItem || activeTasks > 0 || (completed.size() > 0)
    if (!result) {
      printStatistics()
    }
    result
  }

  override def next(): (BlockId, InputStream) = synchronized {
    val now = System.nanoTime()
    while (completed.isEmpty) {
      try {
        wait()
      } catch {
        case _: InterruptedException =>
      }
    }
    timeWaiting += System.nanoTime() - now
    numStreams += 1
    val result = completed.pop()
    memoryUsage -= result._3
    notifyAll()
    return (result._2, result._1)
  }
}
