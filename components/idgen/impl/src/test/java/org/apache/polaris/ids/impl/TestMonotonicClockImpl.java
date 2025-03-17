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
package org.apache.polaris.ids.impl;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.ids.api.MonotonicClock;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
@Timeout(value = 5, unit = MINUTES)
public class TestMonotonicClockImpl {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void simple() {
    var systemNow = System.currentTimeMillis();

    try (var monotonicClock =
        new MonotonicClockImpl() {
          @Override
          long systemNanoTime() {
            return MILLISECONDS.toNanos(systemNow);
          }

          @Override
          long systemCurrentTimeMillis() {
            return systemNow;
          }
        }) {
      monotonicClock.start();
      soft.assertThat(monotonicClock.currentTimeMillis()).isEqualTo(systemNow);
    }
  }

  @Test
  public void realClockMustNotGoBackwards() {
    try (var monotonicClock = new MonotonicClockImpl()) {
      monotonicClock.start();

      var lastMicros = monotonicClock.currentTimeMicros();
      var lastMillis = monotonicClock.currentTimeMillis();
      var lastInstant = monotonicClock.currentInstant();
      var lastNanos = monotonicClock.systemNanoTime();

      // Run for 5 seconds so there is a real chance to catch a couple of "second wraps" and
      // wall-clock changes.
      var endAfter = lastNanos + SECONDS.toNanos(5);

      while (true) {

        var nanos = monotonicClock.systemNanoTime();
        soft.assertThat(nanos).isGreaterThanOrEqualTo(lastNanos);

        var micros = monotonicClock.currentTimeMicros();
        soft.assertThat(micros).isGreaterThanOrEqualTo(lastMicros);

        var millis = monotonicClock.currentTimeMillis();
        soft.assertThat(millis).isGreaterThanOrEqualTo(lastMillis);

        var instant = monotonicClock.currentInstant();
        soft.assertThat(instant).isAfterOrEqualTo(lastInstant);

        soft.assertAll();

        lastMicros = micros;
        lastMillis = millis;
        lastInstant = instant;
        lastNanos = nanos;

        if (nanos > endAfter) {
          break;
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      longs = {
        0L,
        500L,
        1_000L,
        1_000_000L,
        1_000_000_000L,
        // wrap around to negative
        Long.MAX_VALUE,
        // wrap around to negative after 50ms
        Long.MAX_VALUE - 50_000_000L,
        // wrap around to negative after 150ms
        Long.MAX_VALUE,
        // "negative for" 50ms
        -50_000_000L,
        // "negative for" 150ms
        -150_000_000L,
        // "negative for" 500ms
        -500_000_000L,
        // "negative for" 500s
        -500_00_000_000L,
        // always negative
        Long.MIN_VALUE + 100_000_000_000L,
        Long.MIN_VALUE + 100_000_000L,
        Long.MIN_VALUE + 100_000L,
        Long.MIN_VALUE + 1000L,
        Long.MIN_VALUE + 500L,
        Long.MIN_VALUE + 1,
        Long.MIN_VALUE
      })
  public void nanoSourceNegativePositive(long nanoOffset) {
    var nano = nanoOffset;
    var systemWall = 0L;
    var realTimeWall = 0L;
    var inst = Instant.EPOCH;
    var currentMillis = new AtomicLong(systemWall);
    var currentNanos = new AtomicLong(nano);

    // MonotonicClockImpl not started, no need to close()
    @SuppressWarnings("resource")
    var monotonicClock =
        new MonotonicClockImpl() {
          @Override
          long systemCurrentTimeMillis() {
            return currentMillis.get();
          }

          @Override
          long systemNanoTime() {
            return currentNanos.get();
          }
        };

    soft.assertThat(monotonicClock)
        .extracting(
            MonotonicClock::currentTimeMillis,
            MonotonicClock::currentTimeMicros,
            MonotonicClock::currentInstant,
            MonotonicClock::nanoTime)
        .containsExactly(realTimeWall, instantToMicros(inst), inst, nano);

    monotonicClock.tick();

    soft.assertThat(monotonicClock)
        .extracting(
            MonotonicClock::currentTimeMillis,
            MonotonicClock::currentTimeMicros,
            MonotonicClock::currentInstant,
            MonotonicClock::nanoTime)
        .containsExactly(realTimeWall, instantToMicros(inst), inst, nano);

    monotonicClock.tick();

    // -- wall clock too slow
    // Wall clock advanced by 100ms
    // Real time advanced by 200ms
    realTimeWall = 200;
    nano = nanoOffset + MILLISECONDS.toNanos(realTimeWall) + 123456L;
    systemWall = 100;
    inst = Instant.ofEpochSecond(0, MILLISECONDS.toNanos(realTimeWall) + 123456L);
    currentNanos.set(nano);
    currentMillis.set(systemWall);

    monotonicClock.tick();

    soft.assertThat(monotonicClock)
        .extracting(
            MonotonicClock::currentTimeMillis,
            MonotonicClock::currentTimeMicros,
            MonotonicClock::currentInstant,
            MonotonicClock::nanoTime)
        .containsExactly(realTimeWall, instantToMicros(inst), inst, nano);

    realTimeWall = 1400;
    nano = nanoOffset + MILLISECONDS.toNanos(realTimeWall) + 234567L;
    systemWall = 1400;
    inst = Instant.ofEpochSecond(0, MILLISECONDS.toNanos(1400) + 234567L);
    currentNanos.set(nano);
    currentMillis.set(systemWall);

    monotonicClock.tick();

    soft.assertThat(monotonicClock)
        .extracting(
            MonotonicClock::currentTimeMillis,
            MonotonicClock::currentTimeMicros,
            MonotonicClock::currentInstant,
            MonotonicClock::nanoTime)
        .containsExactly(realTimeWall, instantToMicros(inst), inst, nano);

    // wall clock goes backwards
    // wall = 200;
    currentNanos.set(nano);
    systemWall = 1000;
    currentMillis.set(systemWall);

    monotonicClock.tick();

    soft.assertThat(monotonicClock)
        .extracting(
            MonotonicClock::currentTimeMillis,
            MonotonicClock::currentTimeMicros,
            MonotonicClock::currentInstant,
            MonotonicClock::nanoTime)
        .containsExactly(realTimeWall, instantToMicros(inst), inst, nano);

    // wall clock advances
    realTimeWall = 2000;
    systemWall = 2000;
    nano = nanoOffset + MILLISECONDS.toNanos(realTimeWall) + 234567L;
    inst = Instant.ofEpochSecond(0, MILLISECONDS.toNanos(realTimeWall) + 234567L);
    currentNanos.set(nano);
    currentMillis.set(systemWall);

    monotonicClock.tick();

    soft.assertThat(monotonicClock)
        .extracting(
            MonotonicClock::currentTimeMillis,
            MonotonicClock::currentTimeMicros,
            MonotonicClock::currentInstant,
            MonotonicClock::nanoTime)
        .containsExactly(realTimeWall, instantToMicros(inst), inst, nano);
  }

  @Test
  public void currentInstantAndMillis() {
    // Note: this test case emits FAKE "MonotonicClock tick loop is stalled" warnings!
    // Ignore those warnings.

    var currentMillis = new AtomicLong();
    var currentNanos = new AtomicLong();

    // MonotonicClockImpl not started, no need to close()
    @SuppressWarnings("resource")
    var monotonicClock =
        new MonotonicClockImpl() {
          @Override
          long systemCurrentTimeMillis() {
            return currentMillis.get();
          }

          @Override
          long systemNanoTime() {
            return currentNanos.get();
          }
        };

    monotonicClock.tick();

    soft.assertThat(monotonicClock.systemNanoTime()).isEqualTo(0L);
    soft.assertThat(monotonicClock.currentTimeMicros()).isEqualTo(0L);
    soft.assertThat(monotonicClock.currentTimeMillis()).isEqualTo(0L);
    soft.assertThat(monotonicClock.currentInstant().toEpochMilli()).isEqualTo(0L);

    var nanos = 456111222333L;
    var millis = TimeUnit.NANOSECONDS.toMillis(nanos);
    currentMillis.set(millis);
    currentNanos.set(nanos);

    monotonicClock.tick();

    soft.assertThat(monotonicClock.systemNanoTime()).isEqualTo(nanos);
    soft.assertThat(monotonicClock.currentTimeMicros())
        .extracting(MICROSECONDS::toMillis)
        .isEqualTo(millis);
    soft.assertThat(monotonicClock.currentTimeMillis()).isEqualTo(millis);
    soft.assertThat(monotonicClock.currentInstant().toEpochMilli()).isEqualTo(millis);
  }

  @Test
  public void strictlyMonotonicIfWallClockGoesBackwards() {
    var millis = new AtomicLong(System.currentTimeMillis());
    var nanos = new AtomicLong(System.nanoTime());

    var adjustCalled = new AtomicBoolean();

    // MonotonicClockImpl not started, no need to close()
    @SuppressWarnings("resource")
    var monotonicClock =
        new MonotonicClockImpl() {
          @Override
          void afterAdjust() {
            adjustCalled.set(true);
          }

          @Override
          long systemCurrentTimeMillis() {
            return millis.get();
          }

          @Override
          long systemNanoTime() {
            return nanos.get();
          }
        };

    var initial = monotonicClock.currentTimeMillis();

    // Begin ---------------------------------------------------------------------
    //
    // Check that the monotonic clock advances

    adjustCalled.set(false);

    // Increment the nano-clock source by 1 millisecond
    nanos.addAndGet(MILLISECONDS.toNanos(1));

    monotonicClock.tick();

    // Test case:
    var afterWork1 = monotonicClock.currentTimeMillis();
    soft.assertThat(adjustCalled).isFalse();
    soft.assertThat(afterWork1).isGreaterThan(initial);

    // <<End

    // Begin ---------------------------------------------------------------------
    //
    // Check that the monotonic clock fetches the updated wall clock, but disregards it as it
    // went backwards

    adjustCalled.set(false);

    // Let the wall clock go backwards
    millis.addAndGet(-10);
    // Increment the nano-clock source by 1 second
    nanos.addAndGet(SECONDS.toNanos(1));

    monotonicClock.tick();

    // Test case:
    var afterWork2 = monotonicClock.currentTimeMillis();
    soft.assertThat(adjustCalled).isFalse();
    soft.assertThat(afterWork2).isGreaterThan(afterWork1);

    // <<End

    // Begin ---------------------------------------------------------------------
    //
    // Check that the monotonic clock fetches the updated wall clock, but this time uses it, as
    // it went forwards (after the last remembered wall clock)

    adjustCalled.set(false);

    // Let the wall clock go forwards to trigger the wall clock sync
    millis.addAndGet(SECONDS.toMillis(5));
    // Increment the nano-clock source by 1 second
    nanos.addAndGet(SECONDS.toNanos(1));

    monotonicClock.tick();

    // Test case:
    var afterWork3 = monotonicClock.currentTimeMillis();
    soft.assertThat(adjustCalled).isTrue();
    soft.assertThat(afterWork3).isGreaterThan(afterWork2);

    // <<End
  }

  private static long instantToMicros(Instant inst) {
    var microsFromSecond = SECONDS.toMicros(inst.getEpochSecond());
    var microsFromNanoPart = NANOSECONDS.toMicros(inst.getNano());
    return microsFromSecond + microsFromNanoPart;
  }
}
