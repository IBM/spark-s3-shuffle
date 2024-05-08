//
// Copyright 2022- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache 2.0
//

package org.apache.spark.shuffle

import scala.collection.concurrent.TrieMap
import scala.util.Try

class ConcurrentObjectMap[K, V] {
  private val lock = new Object()
  private val valueLocks = new TrieMap[K, Object]()
  private val map = new TrieMap[K, V]()

  def clear(): Unit = {
    lock.synchronized {
      map.clear()
    }
  }

  def getOrElsePut(key: K, op: K => V): V = {
    val l = valueLocks
      .get(key)
      .getOrElse({
        lock.synchronized {
          valueLocks.getOrElseUpdate(
            key, {
              new Object()
            }
          )
        }
      })
    l.synchronized {
      return map.getOrElseUpdate(key, op(key))
    }
  }

  def remove(filter: (K) => Boolean, action: Option[(V) => Unit]): Unit = {
    lock.synchronized {
      val keys = valueLocks.filterKeys(filter)
      keys.foreach(v => {
        val key = v._1
        val l = v._2
        l.synchronized {
          valueLocks.remove(key)
          val obj = map.get(key)
          if (obj.isDefined && action.isDefined) {
            Try(action.get(obj.get))
          }
        }
        map.remove(key)
      })
    }
  }
}
