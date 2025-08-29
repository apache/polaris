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

package org.apache.polaris.service.events.listeners;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class ConcurrentLinkedQueueWithApproximateSize<T> {
  private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger size = new AtomicInteger();

  public void add(T event) {
    queue.add(event);
    size.getAndIncrement();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public T peek() {
    return queue.peek();
  }

  public int size() {
    return size.get();
  }

  public Stream<T> stream() {
    return queue.stream();
  }
}
