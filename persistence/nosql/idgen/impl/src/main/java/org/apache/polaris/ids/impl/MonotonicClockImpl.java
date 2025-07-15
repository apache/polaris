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

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.polaris.ids.api.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monotonic clock implementation that leverages {@link System#nanoTime()} as the primary
 * monotonically increasing time source, provided via {@link #nanoTime()}. {@link
 * System#currentTimeMillis()} is used to provide a monotonically increasing wall clock provided via
 * {@link #currentInstant()}, {@link #currentTimeMicros()} and {@link #currentTimeMillis()}.
 *
 * <p>The implementation starts a single "tick-thread" polling the wall clock to calculate the
 * adjustment that is necessary to provide the values for {@code currentTime*()}.
 *
 * <p>Serving the current instant or "current time micros" however is a bit more complex, as the
 * wall-clock source only has millisecond precision, but the instant has nanosecond precision. This
 * means that the value returned for "current instant" needs to be created from the "nanosecond
 * time" and involving an "adjustment" value. That "adjustment" is also updated by the tick-thread
 * and represents the difference of the current nano-time and system wall clock, considering the
 * fact that the system wall clock can go backwards or forwards or not being updated every
 * millisecond.
 *
 * <p>This implementation expects that the wall clock in nanoseconds since epoch can be represented
 * by the values in the range {@code 0 .. Long.MAX_VALUE}. This implementation <em>must</em> be
 * adapted approaching the year 2262 (approx 292 years fit into this range).
 *
 * <p>Regarding "short-time" Thread.sleep() be aware of <a
 * href="https://bugs.openjdk.org/browse/JDK-8306463">JDK-8306463</a> and <a
 * href="https://bugs.openjdk.org/browse/JDK-8305092">JDK-8305092</a>.
 *
 * <p>Even with very minimal sleep durations, the actual sleep time depends on the OS and in
 * particular its scheduler. Sleep times have been measured to vary between some microseconds up to
 * 2ms.
 */
@ApplicationScoped
@VisibleForTesting
@SuppressWarnings("FieldCanBeStatic")
public class MonotonicClockImpl implements MonotonicClock {
  private static final Logger LOGGER = LoggerFactory.getLogger(MonotonicClockImpl.class);

  private volatile boolean stopTicker;
  private volatile CountDownLatch tickerThreadLatch;

  // TODO should the implementation only advance the wall clock gradually?
  // TODO protect against accidental huge wall clock advances?
  // TODO should the implementation maybe never adjust to an advanced wall-clock? (i.e. faster wall
  //  clock than real time clock)

  // Best-effort to have the volatile fields not in the same cache line as the object header

  @SuppressWarnings("unused")
  private final long _pad_1_0 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_1_1 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_1_2 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_1_3 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_1_4 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_1_5 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_1_6 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_1_7 = 0L;

  private volatile long adjustToWallClockAsNanos;

  @SuppressWarnings("unused")
  private final long _pad_2_0 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_2_1 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_2_2 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_2_3 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_2_4 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_2_5 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_2_6 = 0L;

  @SuppressWarnings("unused")
  private final long _pad_2_7 = 0L;

  private volatile long previousSystemNanoTime;
  private static final AtomicLongFieldUpdater<MonotonicClockImpl>
      PREVIOUS_SYSTEM_NANO_TIME_UPDATER =
          AtomicLongFieldUpdater.newUpdater(MonotonicClockImpl.class, "previousSystemNanoTime");

  @SuppressWarnings("resource")
  @VisibleForTesting
  public static MonotonicClock newDefaultInstance() {
    return new MonotonicClockImpl().start();
  }

  // for CDI

  /** Default "production" constructor. */
  MonotonicClockImpl() {
    setup();
  }

  protected void setup() {
    var nowNanos = systemNanoTime();
    PREVIOUS_SYSTEM_NANO_TIME_UPDATER.set(this, nowNanos);

    var nowWallClockAsMillis = systemCurrentTimeMillis();

    this.adjustToWallClockAsNanos = MILLISECONDS.toNanos(nowWallClockAsMillis) - nowNanos;
  }

  /** Constructor for {@code MutableMonotonicClock}. */
  @SuppressWarnings("unused")
  protected MonotonicClockImpl(boolean dummy) {}

  long currentTimeNanos() {
    return currentTimeNanos(monotonicSystemNanoTime());
  }

  private long currentTimeNanos(long nanos) {
    nanos += this.adjustToWallClockAsNanos;
    return nanos;
  }

  /** Called regularly to adjust to wall-clock drift, if the wall-clock adjust into the future. */
  @VisibleForTesting
  protected void tick() {
    var nowNanos = monotonicSystemNanoTime();
    var nowWallClockAsMillis = systemCurrentTimeMillis();

    var expectedWallClockMillis = NANOSECONDS.toMillis(currentTimeNanos(nowNanos));
    var advancedInMillis = nowWallClockAsMillis - expectedWallClockMillis;

    // Only adjust if wall clock did not go backwards
    if (advancedInMillis > 0) {
      var adjustAsNanos = this.adjustToWallClockAsNanos;
      this.adjustToWallClockAsNanos = adjustAsNanos + MILLISECONDS.toNanos(advancedInMillis);

      afterAdjust();

      // log a if the system wall clock adjustment is quite a sudden bump for a production system
      if (advancedInMillis > 200) {
        var l =
            advancedInMillis > 30_000
                ? LOGGER.atError()
                : (advancedInMillis > 2_000 ? LOGGER.atWarn() : LOGGER.atInfo());
        l.log("System wall clock adjustment advanced by {}", Duration.ofMillis(advancedInMillis));
      }
    }
  }

  @SuppressWarnings("BusyWait")
  private void ticker() {
    try {
      tickerThreadLatch = new CountDownLatch(1);
      while (!stopTicker) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          // ignore
        }
        tick();
      }
    } finally {
      tickerThreadLatch.countDown();
    }
  }

  @PostConstruct
  void startForCDI() {
    checkState(!stopTicker, "Already started");

    var t = new Thread(this::ticker, "Monotonic Clock Thread");
    t.setDaemon(true);
    t.start();
  }

  protected MonotonicClockImpl start() {
    startForCDI();
    return this;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  @PreDestroy
  public void close() {
    stopTicker = true;
    var t = tickerThreadLatch;
    if (t != null) {
      try {
        t.await(1, MINUTES);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        tickerThreadLatch = null;
      }
    }
  }

  @Override
  public long currentTimeMicros() {
    return NANOSECONDS.toMicros(currentTimeNanos());
  }

  @Override
  public long currentTimeMillis() {
    return NANOSECONDS.toMillis(currentTimeNanos());
  }

  @Override
  public Instant currentInstant() {
    var adjustedNanos = currentTimeNanos();

    var seconds = NANOSECONDS.toSeconds(adjustedNanos);
    var nanoPart = adjustedNanos % SECONDS.toNanos(1);

    return Instant.ofEpochSecond(seconds, nanoPart);
  }

  @Override
  public long nanoTime() {
    return monotonicSystemNanoTime();
  }

  @Override
  public void sleepMillis(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void waitUntilTimeMillisAdvanced() {
    var start = currentTimeMillis();
    var now = 0L;
    do {
      try {
        // The minimum interval is (at least up to Java 23 on Linux) is the time it takes the OS
        // scheduler to switch tasks. That time is way higher than one nanosecond.
        Thread.sleep(0, 1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      now = currentTimeMillis();
    } while (start == now);
  }

  // Overridden by tests
  @VisibleForTesting
  protected void afterAdjust() {}

  // Overridden by tests
  @VisibleForTesting
  protected long systemCurrentTimeMillis() {
    return System.currentTimeMillis();
  }

  // Overridden by tests
  @VisibleForTesting
  protected long systemNanoTime() {
    return System.nanoTime();
  }

  /**
   * {@link System#nanoTime() System.nanoTime()} does not guarantee that the values will be
   * monotonically increasing when invocations happen from different CPUs/cores.
   *
   * <p>This function guarantees that the returned value is always equal to or greater than the last
   * returned value.
   *
   * <p>Adding a "simple unit test" for this function is extremely tricky, because every
   * synchronization added to a test "breaks" real concurrency, which is however what needs to be
   * tested.
   */
  private long monotonicSystemNanoTime() {
    while (true) {
      var nanos = systemNanoTime();
      var last = PREVIOUS_SYSTEM_NANO_TIME_UPDATER.get(this);
      var diff = nanos - last;
      // Attention! 'diff' can be negative!
      if (diff > 0L) {
        if (PREVIOUS_SYSTEM_NANO_TIME_UPDATER.compareAndSet(this, last, nanos)) {
          return nanos;
        }
      } else if (diff == 0L) {
        return nanos;
      }
      Thread.onSpinWait();
    }
  }
}
