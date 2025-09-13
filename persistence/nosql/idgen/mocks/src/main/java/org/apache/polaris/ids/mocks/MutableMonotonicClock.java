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
package org.apache.polaris.ids.mocks;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.enterprise.inject.Specializes;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.ids.impl.MonotonicClockImpl;

@Specializes
public class MutableMonotonicClock extends MonotonicClockImpl {
  private final AtomicLong nanoTime = new AtomicLong();
  private final AtomicLong currentTimeMillis = new AtomicLong();

  public MutableMonotonicClock() {
    this(System.currentTimeMillis(), System.nanoTime());
  }

  public MutableMonotonicClock(long currentTimeMillis, long nanoTime) {
    super(false);
    this.currentTimeMillis.set(currentTimeMillis);
    this.nanoTime.set(nanoTime);
    setup();
  }

  @CanIgnoreReturnValue
  public MutableMonotonicClock setCurrentTimeMillis(long currentTimeMillis) {
    this.currentTimeMillis.set(currentTimeMillis);
    return this;
  }

  @CanIgnoreReturnValue
  public MutableMonotonicClock setNanoTime(long nanoTime) {
    this.nanoTime.set(nanoTime);
    return this;
  }

  @CanIgnoreReturnValue
  public MonotonicClockImpl advanceBoth(long time, TimeUnit unit) {
    nanoTime.addAndGet(unit.toNanos(time));
    currentTimeMillis.addAndGet(unit.toMillis(time));
    return this;
  }

  @CanIgnoreReturnValue
  public MonotonicClockImpl advanceBoth(Duration duration) {
    nanoTime.addAndGet(duration.toNanos());
    currentTimeMillis.addAndGet(duration.toMillis());
    return this;
  }

  @CanIgnoreReturnValue
  public MonotonicClockImpl advanceNanos(long time, TimeUnit unit) {
    nanoTime.addAndGet(unit.toNanos(time));
    return this;
  }

  @CanIgnoreReturnValue
  public MonotonicClockImpl advanceNanos(Duration duration) {
    nanoTime.addAndGet(duration.toNanos());
    return this;
  }

  @CanIgnoreReturnValue
  public MonotonicClockImpl advanceCurrentTimeMillis(long time, TimeUnit unit) {
    currentTimeMillis.addAndGet(unit.toMillis(time));
    return this;
  }

  @CanIgnoreReturnValue
  public MonotonicClockImpl advanceCurrentTimeMillis(Duration duration) {
    currentTimeMillis.addAndGet(duration.toMillis());
    return this;
  }

  @Override
  public long systemCurrentTimeMillis() {
    return currentTimeMillis.get();
  }

  @Override
  public long systemNanoTime() {
    return nanoTime.get();
  }
}
