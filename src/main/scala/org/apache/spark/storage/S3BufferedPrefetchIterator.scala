//
// Copyright 2023- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache 2.0
//

package org.apache.spark.storage

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.helper.S3ShuffleDispatcher

import java.io.{BufferedInputStream, InputStream}
import java.util
import java.util.concurrent.atomic.AtomicLong

class S3BufferedPrefetchIterator(iter: Iterator[(BlockId, S3ShuffleBlockStream)], maxBufferSize: Long) extends Iterator[(BlockId, InputStream)] with Logging {
  private val startTime = System.nanoTime()

  @volatile private var memoryUsage: Long = 0
  @volatile private var hasItem: Boolean = iter.hasNext
  private var timeWaiting: Long = 0
  private var timePrefetching: Long = 0
  private var numStreams: Long = 0
  private var bytesRead: Long = 0

  private var activeTasks: Long = 0

  private val completed = new util.LinkedList[(InputStream, BlockId, Long)]()

  private class ThreadPredictor(maxThreads: Int) {
    private var currentThreads = 1
    private val latencies = Array.fill(maxThreads + 2)(0.toLong)
    latencies(0) = Long.MaxValue
    latencies(maxThreads + 1) = Long.MaxValue

    private var numMeasurements = 0
    private val measurementsNS = Array.fill(20)(0.toLong)

    private def predict(): Int = synchronized {
      if (numMeasurements < measurementsNS.length + currentThreads) {
        return currentThreads
      }
      val current = measurementsNS.sum
      if (current < 500) { // Less than 25ns latency for each request.
        return currentThreads
      }
      latencies(currentThreads) = current
      val prevValue = latencies(currentThreads - 1)
      val nextValue = latencies(currentThreads + 1)

      numMeasurements = 0
      if (prevValue < current) {
        currentThreads -= 1
      } else if (nextValue < current) {
        currentThreads += 1
      }
      currentThreads
    }

    def addMeasurementAndPredict(latencyNS: Long): Int = synchronized {
      if (latencyNS >= 0) {
        measurementsNS(numMeasurements % measurementsNS.length) = latencyNS
        numMeasurements += 1
      }
      predict()
    }
  }

  private val threadPredictor = new ThreadPredictor(S3ShuffleDispatcher.get.maxConcurrencyTask)

  private val ptr = this
  private val currentActiveThreads = new AtomicLong(0)
  private val desiredActiveThreads = new AtomicLong(0)

  // Configure the threads based on the wait time.
  private def configureThreads(latency: Long): Unit = synchronized {
    if (desiredActiveThreads.get() != currentActiveThreads.get()) {
      return
    }
    val nThreads = threadPredictor.addMeasurementAndPredict(latency)
    val activeThreads = desiredActiveThreads.getAndSet(nThreads)
    if (nThreads > activeThreads) {
      val t = new Thread {
        override def run(): Unit = {
          ptr.prefetchThread(nThreads)
        }
      }
      t.start()
    }
  }
  // Make sure that there's at least a single thread running.
  configureThreads(-1)

  private def onCloseStream(bufferSize: Int): Unit = synchronized {
    // Reduce the memory usage once the stream has been closed and the buffer was made available.
    memoryUsage -= bufferSize
    notifyAll()
  }

  private def prefetchThread(threadId: Long): Unit = {
    currentActiveThreads.incrementAndGet()
    var nextElement: (BlockId, S3ShuffleBlockStream) = null
    while (true) {
      synchronized {
        if (!iter.hasNext && nextElement == null) {
          hasItem = false
          return
        }
        if (nextElement == null) {
          if (threadId > desiredActiveThreads.get()) {
            currentActiveThreads.decrementAndGet()
            return
          }
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
        val stream = new S3BufferedInputStreamAdaptor(s, bsize, onCloseStream)
        timePrefetching += System.nanoTime() - now
        bytesRead += bsize
        synchronized {
          completed.push((stream, block, bsize))
          activeTasks -= 1
          notifyAll()
        }
      }
    }
    currentActiveThreads.decrementAndGet()
  }

  private def printStatistics(): Unit = synchronized {
    val totalRuntime = System.nanoTime() - startTime
    val tc = TaskContext.get()
    val sId = tc.stageId()
    val sAt = tc.stageAttemptNumber()
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
      // Threads
      val ta = desiredActiveThreads.get()
      logInfo(s"Statistics: Stage ${sId}.${sAt} TID ${tc.taskAttemptId()} -- " +
                s"${bR} bytes, ${tW} ms waiting (${atW} avg), " +
                s"${tP} ms prefetching (avg: ${atP} ms - ${bs} block size - ${bW} MiB/s). " +
                s"Total: ${tR} ms - ${wPer}% waiting. ${ta} active threads.")
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
    val latency = System.nanoTime() - now
    configureThreads(latency)
    timeWaiting += latency
    numStreams += 1
    val result = completed.pop()
    notifyAll()
    return (result._2, result._1)
  }
}
