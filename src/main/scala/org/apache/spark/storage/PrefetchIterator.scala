/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import scala.collection.AbstractIterator

class PrefetchIterator[A](iter: Iterator[A]) extends AbstractIterator[A] {

  private var value: A = _
  private var valueDefined: Boolean = false

  override def hasNext: Boolean = {
    populateNext()
    valueDefined
  }

  private def populateNext(): Unit = {
    if (valueDefined) {
      return
    }
    if (iter.hasNext) {
      value = iter.next()
      valueDefined = true
    }
  }

  override def next(): A = {
    val result = value
    valueDefined = false
    populateNext()
    result
  }
}