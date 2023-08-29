/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BufferedAsyncPrefetchIterator.scalingFactor
import org.apache.spark.util.ThreadUtils

import java.io.{BufferedInputStream, InputStream}
import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

class BufferedAsyncPrefetchIterator(iter: Iterator[(BlockId, S3ShuffleBlockStream)], maxBufferSize: Long) extends Iterator[(BlockId, InputStream)] with Logging {
  @volatile private var memoryUsage: Long = 0
  @volatile private var error: Throwable = null
  private var timeWaiting: Long = 0
  private val timePrefetching = new AtomicLong(0)
  private val timeTasks = new AtomicLong(0)
  private var numStreams: Long = 0
  private val bytesRead = new AtomicLong(0)

  private var activeTasks: Long = 0
  private var nextElement: (BlockId, S3ShuffleBlockStream) = null
  private val completed = new util.LinkedList[(InputStream, BlockId, Int)]()

  private def addSingleTask(): Boolean = {
    var futBSize: Int = 0
    var futBlockId: BlockId = null
    var futStream: S3ShuffleBlockStream = null

    synchronized {
      if (memoryUsage > maxBufferSize || activeTasks >= scalingFactor) {
        return false
      }
      if (!iter.hasNext && nextElement == null) {
        return false
      }
      if (nextElement == null) {
        nextElement = iter.next()
      }
      futBlockId = nextElement._1
      futStream = nextElement._2
      futBSize = scala.math.min(maxBufferSize, futStream.maxBytes).toInt
      if (memoryUsage + math.min(futBSize, maxBufferSize) > maxBufferSize) {
        return false
      }
      nextElement = null
      memoryUsage += futBSize
      activeTasks += 1
    }

    val fut = Future {
      val now = System.nanoTime()
      val stream = new BufferedInputStream(futStream, futBSize)

      // Fill the buffered input stream by reading and then resetting the stream.
      stream.mark(futBSize)
      stream.read()
      stream.reset()
      timePrefetching.getAndAdd(System.nanoTime() - now)
      bytesRead.getAndAdd(futBSize)

      (stream, futBlockId, futBSize)
    }(BufferedAsyncPrefetchIterator.asyncExecutionContext)
    fut.onComplete(onComplete)(BufferedAsyncPrefetchIterator.asyncExecutionContext)
    true
  }

  private def addTasks(): Unit = {
    (1 to scalingFactor).foreach(_ => {
      val success = addSingleTask()
      if (!success) {
        return
      }

    })
  }
  addTasks()

  def onComplete(result: Try[(InputStream, BlockId, Int)]): Unit = result match {
    case Failure(exception) => this.synchronized {
      error = exception
    }
    case Success(value) => {
      this.synchronized({
        completed.push(value)
        activeTasks -= 1
        notifyAll()
      })
    }
  }

  private def printStatistics(): Unit = synchronized {
    try {
      val tW = timeWaiting / 1000000
      val tP = timePrefetching.get() / 1000000
      val bR = bytesRead.get()
      val r = numStreams
      // Average time per read
      val tR = tP / r
      // Average read bandwidth
      val bW = bR.toDouble / (tP.toDouble / 1000) / (1024 * 1024)
      // Block size
      val bs = bR / r

      val tT = timeTasks.get() / 1000000
      logInfo(s"Statistics: ${bR} bytes buffered, ${tW} ms waiting, ${tP} ms prefetching ${tT} ms for tasks (on avg: ${tR} ms waiting request - ${bs} block size - ${bW} MiB/s)")
    } catch {
      case e: Exception => logError(f"Unable to print statistics: ${e.getMessage}.")
    }
  }

  override def hasNext: Boolean = synchronized {
    val result = iter.hasNext || activeTasks > 0 || completed.size() > 0
    if (!result) {
      printStatistics()
    }
    result
  }

  override def next(): (BlockId, InputStream) = {
    var result: (InputStream, BlockId, Int) = null
    synchronized {
      if (error != null) {
        throw error
      }
      val now = System.nanoTime()
      while (completed.isEmpty) {
        try {
          wait()
        } catch {
          case _: InterruptedException =>
            Thread.currentThread.interrupt()
        }
      }
      timeWaiting += System.nanoTime() - now
      numStreams += 1
      result = completed.pop()
      memoryUsage -= result._3
    }
    val now = System.nanoTime()
    addTasks()
    timeTasks.getAndAdd(System.nanoTime() - now)
    (result._2, result._1)
  }
}

object BufferedAsyncPrefetchIterator {
  private val scalingFactor = 3
  private lazy val asyncThreadPool = ThreadUtils.newDaemonCachedThreadPool("buffered-async-prefetcher", Runtime.getRuntime.availableProcessors() * scalingFactor)
  private lazy implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)
}