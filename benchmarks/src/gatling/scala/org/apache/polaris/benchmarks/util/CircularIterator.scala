/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.benchmarks.util

import scala.util.Random

class CircularIterator[T](builder: () => Iterator[T]) extends Iterator[T] {
  private var currentIterator: Iterator[T] = builder()

  override def hasNext: Boolean = true

  override def next(): T = synchronized {
    if (!currentIterator.hasNext) {
      currentIterator = builder()
    }
    currentIterator.next()
  }
}

class BufferedRandomIterator[T](underlying: CircularIterator[T], bufferSize: Int)
    extends Iterator[T] {
  private var buffer: Iterator[T] = populateAndShuffle()

  private def populateAndShuffle(): Iterator[T] =
    Random.shuffle((1 to bufferSize).map(_ => underlying.next()).toList).iterator

  override def hasNext: Boolean = true

  override def next(): T = synchronized {
    if (!buffer.hasNext) {
      buffer = populateAndShuffle()
    }
    buffer.next()
  }
}
