package org.apache.spark.storage

class StatisticsIterator[A](iter: Iterator[A], fun: () => Unit) extends Iterator[A] {
  override def hasNext: Boolean = {
    if (!iter.hasNext) {
      fun()
      return false
    }
    true
  }

  override def next(): A = iter.next()
}
