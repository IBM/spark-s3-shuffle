package org.apache.spark.shuffle

import org.apache.spark.internal.Logging

import java.io.{IOException, OutputStream}

class S3MeasureOutputStream(var out: OutputStream, label: String = "") extends OutputStream with Logging {
  private var isOpen = true

  private var timings: Long = 0
  private var bytes: Long = 0


  private def checkOpen(): Unit = {
    if (!isOpen) {
      throw new IOException("The stream is already closed!")
    }
  }

  override def write(b: Int): Unit = synchronized {
    checkOpen()
    val now = System.nanoTime()
    out.write(b)
    timings += System.nanoTime() - now
    bytes += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = synchronized {
    checkOpen()
    val now = System.nanoTime()
    out.write(b, off, len)
    timings += System.nanoTime() - now
    bytes += len
  }

  override def flush(): Unit = synchronized {
    checkOpen()
    val now = System.nanoTime()
    out.flush()
    timings += System.nanoTime() - now
  }

  override def close(): Unit = synchronized {
    if (!isOpen) {
      return
    }
    val now = System.nanoTime()
    out.flush()
    out.close()
    timings += System.nanoTime() - now
    out = null
    isOpen = false
    super.close()

    val t = timings / 1000000
    val bw = bytes.toDouble / (t.toDouble / 1000) / (1024 * 1024)
    logInfo(s"Statistics: Writing ${label} ${bytes} took ${t} ms (${bw} MiB/s)")
  }
}
