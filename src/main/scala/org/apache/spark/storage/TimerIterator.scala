/**
 * Copyright 2022- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

class TimerIterator[A](iter: Iterator[A], onNext: (Long) => Unit, finished: () => Unit) extends Iterator[A] {
  override def hasNext: Boolean = {
    val res = iter.hasNext
    if (!res) {
      finished()
    }
    res
  }

  override def next(): A = {
    val now = System.nanoTime()
    val res = iter.next()
    onNext(System.nanoTime() - now)
    res
  }
}
