/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle.sort

import java.io.{IOException, OutputStream}

/**
 * Wrapped ShuffleOutputStream that counts the number of bytes written to it and implements a
 * fake "close"-operation.
 * This allows reuse of the outputStream to store merged-blockfiles.
 */
class S3ShuffleOutputStream(outputStream: OutputStream) extends OutputStream {
  private var byteCount: Long = 0
  private var isClosed = false

  def getNumBytesWritten: Long = byteCount

  override def write(b: Int): Unit = {
    if (isClosed) {
      throw new IOException("S3ShuffleOutputStream is already closed.")
    }
    outputStream.write(b)
    byteCount += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (isClosed) {
      throw new IOException("S3ShuffleOutputStream is already closed.")
    }
    outputStream.write(b, off, len)
    byteCount += len
  }

  override def flush(): Unit = {
    if (isClosed) {
      throw new IOException("S3ShuffleOutputStream is already closed.")
    }
    outputStream.flush()
  }

  override def close(): Unit = {
    isClosed = true
  }
}