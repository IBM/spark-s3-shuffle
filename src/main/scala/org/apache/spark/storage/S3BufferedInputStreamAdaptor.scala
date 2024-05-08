package org.apache.spark.storage

import org.apache.spark.internal.Logging

import java.io.{BufferedInputStream, EOFException, InputStream}

class S3BufferedInputStreamAdaptor(inputStream: InputStream, bufferSize: Int, onClose: (Int) => Unit)
    extends InputStream
    with Logging {

  private var bufferedStream = new BufferedInputStream(inputStream, bufferSize)

  private def prefill(): Unit = synchronized {
    checkOpen()
    // Fill the buffered input stream by reading and then resetting the stream.
    bufferedStream.mark(bufferSize)
    bufferedStream.read()
    bufferedStream.reset()
  }

  prefill()

  private def checkOpen(): Unit = {
    if (bufferedStream == null) {
      throw new EOFException("Stream is closed")
    }
  }

  def read(): Int = synchronized {
    checkOpen()
    bufferedStream.read()
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = synchronized {
    checkOpen()
    bufferedStream.read(b, off, len)
  }

  override def skip(n: Long): Long = synchronized {
    checkOpen()
    bufferedStream.skip(n)
  }

  override def available(): Int = synchronized {
    checkOpen()
    bufferedStream.available()
  }

  override def close(): Unit = synchronized {
    if (bufferedStream == null) {
      logWarning("Double close detected. Ignoring.")
      return
    }
    bufferedStream.close()
    bufferedStream = null
    onClose(bufferSize)
    super.close()
  }
}
