/**
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache2.0
 */

package org.apache.spark.storage

import scala.collection.AbstractIterator
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class AsyncPrefetchIterator[A](iter: Iterator[A]) extends AbstractIterator[A] {

  private var error: Option[Throwable] = Option.empty
  private var value: Option[A] = Option.empty
  private var nextValue: Boolean = false

  override def hasNext: Boolean = synchronized {
    populateNext()
    nextValue
  }

  private def onComplete(result: Try[A]): Unit = synchronized {
    result match {
      case Failure(err) => error = Some(err)
      case Success(v) =>
        value = Some(v)
        notifyAll()
    }
  }

  private def populateNext(): Unit = synchronized {
    if (nextValue) {
      return
    }
    if (iter.hasNext) {
      val fut = Future[A] {
        iter.next()
      }
      fut.onComplete(onComplete)
      nextValue = true
    }
  }

  override def next(): A = synchronized {
    while (value.isEmpty) {
      if (error.isDefined) {
        throw error.get
      }
      try {
        wait()
      }
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
    val result = value.get
    value = Option.empty
    nextValue = false
    populateNext()
    result
  }
}