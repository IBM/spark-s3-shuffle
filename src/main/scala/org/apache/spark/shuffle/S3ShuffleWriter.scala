package org.apache.spark.shuffle

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.FallbackStorage

class S3ShuffleWriter[K, V](writer: ShuffleWriter[K, V]) extends ShuffleWriter[K, V] with Logging {
  override def write(records: Iterator[Product2[K, V]]): Unit = writer.write(records)

  override def stop(success: Boolean): Option[MapStatus] = {
    val message = writer.stop(success)
    if (message.isEmpty) {
      return message
    }
    val status = message.get
    status.updateLocation(FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)
    Option(status)
  }

  override def getPartitionLengths(): Array[Long] = writer.getPartitionLengths()
}
