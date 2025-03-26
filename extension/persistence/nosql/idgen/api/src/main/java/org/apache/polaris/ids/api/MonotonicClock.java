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
package org.apache.polaris.ids.api;

import java.time.Instant;

/**
 * Provides a clock providing the current time in milliseconds, microseconds and instant since
 * 1970-01-01-00:00:00.000. The returned timestamp values increase monotonically.
 *
 * <p>The functions provide nanosecond/microsecond/millisecond precision, but not necessarily the
 * same resolution (how frequently the value changes) - no guarantees are made.
 *
 * <p>Implementation <em>may</em> adjust to wall clocks advancing faster than the real time. If and
 * how exactly depends on the implementation, as long as none of the time values available via this
 * interface "goes backwards".
 *
 * <p>Implementer notes: {@link System#nanoTime() System.nanoTime()} does not guarantee that the
 * values will be monotonically increasing when invocations happen from different
 * CPUs/cores/threads.
 *
 * <p>A default implementation of {@link MonotonicClock} can be injected as an application scoped
 * bean in CDI.
 */
public interface MonotonicClock extends AutoCloseable {
  /**
   * Current timestamp as microseconds since epoch, can be used as a monotonically increasing wall
   * clock.
   */
  long currentTimeMicros();

  /**
   * Current timestamp as milliseconds since epoch, can be used as a monotonically increasing wall
   * clock.
   */
  long currentTimeMillis();

  /**
   * Current instant with nanosecond precision, can be used as a monotonically increasing wall
   * clock.
   */
  Instant currentInstant();

  /** Monotonically increasing timestamp with nanosecond precision, not related to wall clock. */
  long nanoTime();

  void sleepMillis(long millis);

  @Override
  void close();

  void waitUntilTimeMillisAdvanced();
}
