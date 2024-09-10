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
package org.apache.polaris.service.ratelimiting;

import io.opentelemetry.sdk.common.Clock;

/** Implementation of the opentelemetry Clock interface using the Polaris Clock interface */
public class OpenTelemetryClock implements Clock {
  org.apache.polaris.service.ratelimiting.Clock clock;

  public OpenTelemetryClock(org.apache.polaris.service.ratelimiting.Clock clock) {
    this.clock = clock;
  }

  /**
   * Returns the current epoch timestamp in nanos from this clock. This timestamp should only be
   * used to compute a current time. To compute a duration, timestamps should always be obtained
   * using {@link #nanoTime()}. For example, this usage is correct.
   *
   * <pre>{@code
   * long startNanos = clock.nanoTime();
   * // Spend time...
   * long durationNanos = clock.nanoTime() - startNanos;
   * }</pre>
   *
   * <p>This usage is NOT correct.
   *
   * <pre>{@code
   * long startNanos = clock.now();
   * // Spend time...
   * long durationNanos = clock.now() - startNanos;
   * }</pre>
   *
   * <p>Calling this is equivalent to calling {@link #now(boolean)} with {@code highPrecision=true}.
   */
  @Override
  public long now() {
    return clock.nanoTime();
  }

  /**
   * Returns a time measurement with nanosecond precision that can only be used to calculate elapsed
   * time.
   */
  @Override
  public long nanoTime() {
    return clock.nanoTime();
  }
}
