/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import org.apache.spark.internal.Logging

import java.io.{BufferedInputStream, InputStream}
import java.util

class S3BufferedPrefetchIterator(iter: Iterator[(BlockId, S3ShuffleBlockStream)], maxBufferSize: Long) extends Iterator[(BlockId, InputStream)] with Logging {
  @volatile private var memoryUsage: Long = 0
  @volatile private var hasItem: Boolean = iter.hasNext
  private var timeWaiting: Long = 0
  private var timePrefetching: Long = 0
  private var timeNext: Long = 0
  private var numStreams: Long = 0
  private var bytesRead: Long = 0

  private var nextElement: (BlockId, S3ShuffleBlockStream) = null

  private val completed = new util.LinkedList[(InputStream, BlockId, Long)]()

  private def prefetchThread(): Unit = {
    while (iter.hasNext || nextElement != null) {
      if (nextElement == null) {
        val now = System.nanoTime()
        nextElement = iter.next()
        timeNext = System.nanoTime() - now
      }
      val bsize = scala.math.min(maxBufferSize, nextElement._2.maxBytes).toInt

      var fetchNext = false
      synchronized {
        if (memoryUsage + math.min(bsize, maxBufferSize) > maxBufferSize) {
          try {
            wait()
          }
          catch {
            case _: InterruptedException =>
          }
        } else {
          fetchNext = true
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
          memoryUsage += bsize
          completed.push((stream, block, bsize))
          hasItem = iter.hasNext
          notify()
        }
      }
    }
  }

  private val self = this
  private val thread = new Thread {
    override def run(): Unit = {
      self.prefetchThread()
    }
  }
  thread.start()

  private def printStatistics(): Unit = synchronized {
    try {
      val tW = timeWaiting / 1000000
      val tP = timePrefetching / 1000000
      val tN = timeNext / 1000000
      val bR = bytesRead
      val r = numStreams
      // Average time per prefetch
      val atP = tP / r
      // Average time waiting
      val atW = tW / r
      // Average time next
      val atN = tN / r
      // Average read bandwidth
      val bW = bR.toDouble / (tP.toDouble / 1000) / (1024 * 1024)
      // Block size
      val bs = bR / r
      logInfo(s"Statistics: ${bR} bytes, ${tW} ms waiting (${atW} avg), " +
                s"${tP} ms prefetching (avg: ${atP} ms - ${bs} block size - ${bW} MiB/s) " +
                s"${tN} ms for next (${atN} avg)")
    } catch {
      case e: Exception => logError(f"Unable to print statistics: ${e.getMessage}.")
    }
  }

  override def hasNext: Boolean = synchronized {
    val result = hasItem || (completed.size() > 0)
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
