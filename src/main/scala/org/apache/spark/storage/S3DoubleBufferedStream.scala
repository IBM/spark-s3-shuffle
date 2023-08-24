/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import org.apache.hadoop.io.ElasticByteBufferPool
import org.apache.spark.SparkException
import org.apache.spark.shuffle.helper.S3ShuffleDispatcher
import org.apache.spark.storage.S3DoubleBufferedStream.{getBuffer, getBufferSize, releaseBuffer, updateRTT}

import java.io.{EOFException, InputStream}
import java.nio.ByteBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class S3DoubleBufferedStream(stream: S3ShuffleBlockStream, bufferSize: Int) extends InputStream {
  private var buffers: Array[ByteBuffer] = {
    val array = new Array[ByteBuffer](2)
    array(0) = getBuffer(bufferSize)
    array(1) = getBuffer(bufferSize)
    // Mark buffers as empty
    array.foreach(b => {
      b.clear().limit(0)
    })
    array
  }

  var streamClosed = false
  var pos: Long = 0
  val maxBytes: Long = stream.maxBytes

  private var bufIdx: Int = 0
  var dataAvailable: Boolean = false
  var error: Option[Throwable] = None

  doPrefetch(nextBuffer)

  private def currentBuffer: ByteBuffer = synchronized {
    buffers(bufIdx)
  }

  private def nextBuffer: ByteBuffer = synchronized {
    buffers((bufIdx + 1) % buffers.length)
  }

  private def swap() = synchronized {
    bufIdx = (bufIdx + 1) % buffers.length
  }

  private def eof: Boolean = synchronized {
    if (buffers == null) {
      throw new EOFException("Stream already closed")
    }
    pos >= maxBytes
  }

  private def prepareRead(): Unit = synchronized {
    if (!currentBuffer.hasRemaining && dataAvailable) {
      swap()
      dataAvailable = false
      doPrefetch(nextBuffer)
    }
  }

  private def doPrefetch(buffer: ByteBuffer): Unit = {
    if (stream.available() == 0) {
      // no data available
      return
    }
    // Run on implicit global execution context.
    val fut = Future[Int] {
      buffer.clear()
      var len: Int = 0
      do {
        val bufferSize = getBufferSize()
        len = writeTo(buffer, stream, bufferSize)
        if (len < 0) {
          throw new EOFException()
        }
      } while (len == 0)
      buffer.flip()
      len
    }
    fut.onComplete(onCompletePrefetch)
  }

  private def onCompletePrefetch(result: Try[Int]): Unit = synchronized {
    result match {
      case Failure(exception) => error = Some(exception)
      case Success(len) =>
        dataAvailable = true
    }
    notifyAll()
  }

  override def read(): Int = synchronized {
    if (eof) {
      return -1
    }
    while (error.isEmpty) {
      if (buffers == null) {
        throw new EOFException("Stream already closed")
      }
      val now = System.nanoTime()
      prepareRead()
      if (currentBuffer.hasRemaining) {
        val l = readFrom(currentBuffer)
        if (l < 0) {
          throw new SparkException("Invalid state in shuffle read.")
        }
        updateRTT(System.nanoTime() - now)
        pos += 1
        return l
      }
      try {
        wait()
      }
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
    throw error.get
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = synchronized {
    if (off < 0 || len < 0 || off + len < 0 || off + len > b.length) {
      throw new IndexOutOfBoundsException()
    }
    if (eof) {
      return -1
    }
    while (error.isEmpty) {
      if (buffers == null) {
        throw new EOFException("Stream already closed")
      }
      val now = System.nanoTime()
      prepareRead()
      if (currentBuffer.hasRemaining) {
        val l = readFrom(currentBuffer, b, off, len)
        if (l < 0) {
          throw new SparkException("Invalid state in shuffle read(buf).")
        }
        updateRTT(System.nanoTime() - now)
        pos += l
        return l
      }
      try {
        wait()
      }
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
    throw error.get
  }

  override def available(): Int = synchronized {
    if (buffers == null) {
      throw new EOFException("Stream already closed")
    }
    prepareRead()
    currentBuffer.remaining
  }

  override def skip(n: Long): Long = synchronized {
    if (eof) {
      throw new EOFException("Stream already closed")
    }
    if (n <= 0) {
      return 0
    }
    if (n <= currentBuffer.remaining) {
      val len = skipIn(currentBuffer, n.toInt)
      pos += len
      return len
    }
    val maxSkip = math.min(n, maxBytes - pos)
    val skippedFromBuffer = currentBuffer.remaining
    val skipFromStream = maxSkip - skippedFromBuffer
    currentBuffer.limit(0)
    val skipped = skippedFromBuffer + stream.skip(skipFromStream)
    pos += skipped
    skipped
  }

  override def close(): Unit = synchronized {
    if (buffers == null) {
      return
    }
    buffers.foreach(b => releaseBuffer(b))
    stream.close()
    // Release buffers
    buffers = null
  }

  private def skipIn(buf: ByteBuffer, n: Int): Int = {
    val l = math.min(n, buf.remaining())
    buf.position(buf.position() + l)
    l
  }

  private def readFrom(buf: ByteBuffer, dst: Array[Byte], off: Int, len: Int): Int = {
    val length = math.min(len, buf.remaining())
    System.arraycopy(buf.array(), buf.position() + buf.arrayOffset(), dst, off, length)
    buf.position(buf.position() + length)
    length
  }

  private def readFrom(buf: ByteBuffer): Int = {
    if (!buf.hasRemaining) {
      return -1
    }
    buf.get() & 0xFF
  }

  private def writeTo(buf: ByteBuffer, src: InputStream, size: Int): Int = {
    val len = src.read(buf.array(), buf.position() + buf.arrayOffset(), math.min(buf.remaining(), size))
    buf.position(buf.position() + len)
    len
  }
}

object S3DoubleBufferedStream {
  private val bufferTimings = new Array[Double](100)
  private var bufferSize: Int = 8192
  private var bufferPos: Int = 0
  private var bufferCount: Int = 0
  private var weightedAverageRTT: Double = 0.0

  private lazy val bufferSensitivity = S3ShuffleDispatcher.get.bufferSensitivity
  private lazy val minimumReadSize = S3ShuffleDispatcher.get.minimumReadSize

  private def updateRTT(ns: Long): Unit = synchronized {
    if (bufferCount < bufferTimings.length) {
      bufferCount += 1
    }
    bufferPos = (bufferPos + 1) % bufferTimings.length
    bufferTimings(bufferPos) = ns.toDouble
  }

  /**
   * Optimize the read size for a weighted mean of the RTT.
   */
  private def getBufferSize(): Int = synchronized {
    if (bufferCount == 0) {
      return bufferSize
    }
    var wsum: Double = 0
    var mean: Double = 0
    val nTimings = bufferTimings.length
    val nFloat: Float = nTimings.toFloat

    // Compute weighted arithmetic mean of the RoundTripTime on the window.
    for (i <- Range(0, nTimings)) {
      val pos = (bufferPos + i) % nTimings
      val w: Double = ((nFloat - i) / nFloat) * ((nFloat - i) / nFloat)
      mean += bufferTimings(pos) * w
      wsum += w
    }
    val currentWeightedRTT = mean / wsum

    val buffersizePrev = bufferSize
    // Adapt buffer size.
    val diff = weightedAverageRTT - currentWeightedRTT
    if (diff > bufferSensitivity) {
      bufferSize = bufferSize - minimumReadSize
    } else if (diff < -bufferSensitivity) {
      bufferSize = bufferSize * 2
    } 
    // else if (diff.abs > Double.MinPositiveValue && currentWeightedRTT > bufferSensitivity) {
      // bufferSize += minimumReadSize
    // }

    weightedAverageRTT = currentWeightedRTT
    bufferSize = math.min(math.max(bufferSize, minimumReadSize), S3ShuffleDispatcher.get.bufferSize)

    if (bufferSize != buffersizePrev) {
      println(s"New buffer size: ${bufferSize} (diff: ${diff} rtt: ${weightedAverageRTT})")
    }
    bufferSize
  }

  private lazy val pool = new ElasticByteBufferPool()

  private def getBuffer(size: Int): ByteBuffer = pool.getBuffer(false, size)

  private def releaseBuffer(buf: ByteBuffer): Unit = pool.putBuffer(buf)
}