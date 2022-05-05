/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.shuffle.sort

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.api.ShufflePartitionWriter

import java.io.OutputStream

/**
 * Implements ShufflePartitionWriter interface.
 *
 * Note: The outputStream is potentially reused by other writers (see contract in ShufflePartitionWriter.java).
 * Thus we wrap it with the S3ShuffleOutputStream.
 */
class S3ShufflePartitionWriter(outputStream: OutputStream) extends ShufflePartitionWriter with Logging {
  private val wrappedStream = new S3ShuffleOutputStream(outputStream)

  override def openStream(): OutputStream = wrappedStream

  override def getNumBytesWritten: Long = wrappedStream.getNumBytesWritten

  def commit(): Unit = {
    // Noop.
  }

  def close(): Unit = wrappedStream.close()
}
