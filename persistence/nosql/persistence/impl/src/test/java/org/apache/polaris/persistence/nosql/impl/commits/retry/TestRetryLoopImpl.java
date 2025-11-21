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
package org.apache.polaris.persistence.nosql.impl.commits.retry;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.commit.FairRetriesType;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;
import org.apache.polaris.persistence.nosql.api.commit.RetryTimeoutException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;

@ExtendWith(SoftAssertionsExtension.class)
public class TestRetryLoopImpl {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void retryTimeout() {
    var retries = 3;
    var mockedConfig = mockedConfig(retries, Integer.MAX_VALUE);

    var clock = mockedClock(retries);
    var tryLoopState = new RetryLoopImpl<>(mockedConfig, clock);

    var retryCounter = new AtomicInteger();

    soft.assertThatThrownBy(
            () ->
                tryLoopState.retryLoop(
                    (long nanosRemaining) -> {
                      retryCounter.incrementAndGet();
                      return Optional.empty();
                    }))
        .isInstanceOf(RetryTimeoutException.class)
        .asInstanceOf(type(RetryTimeoutException.class))
        .extracting(RetryTimeoutException::getRetry, RetryTimeoutException::getTimeNanos)
        .containsExactly(3, 0L);
    soft.assertThat(retryCounter).hasValue(1 + retries);
  }

  @Test
  public void retryImmediateSuccess() {
    var retries = 3;
    var mockedConfig = mockedConfig(retries, Integer.MAX_VALUE);

    var clock = mockedClock(retries);
    var tryLoopState = new RetryLoopImpl<String>(mockedConfig, clock);

    var retryCounter = new AtomicInteger();
    var result = new AtomicReference<String>();

    soft.assertThatCode(
            () ->
                result.set(
                    tryLoopState.retryLoop(
                        (long nanosRemaining) -> {
                          retryCounter.incrementAndGet();
                          return Optional.of("foo");
                        })))
        .doesNotThrowAnyException();

    soft.assertThat(retryCounter).hasValue(1);
    soft.assertThat(result).hasValue("foo");
  }

  @Test
  public void retry() {
    var retries = 3;
    var mockedConfig = mockedConfig(retries, Integer.MAX_VALUE);

    var clock = mockedClock(retries);
    var tryLoopState = new RetryLoopImpl<String>(mockedConfig, clock);

    var retryCounter = new AtomicInteger();
    var result = new AtomicReference<String>();

    soft.assertThatCode(
            () ->
                result.set(
                    tryLoopState.retryLoop(
                        (long nanosRemaining) -> {
                          if (retryCounter.incrementAndGet() == 1) {
                            return Optional.empty();
                          }
                          return Optional.of("foo");
                        })))
        .doesNotThrowAnyException();

    soft.assertThat(retryCounter).hasValue(2);
    soft.assertThat(result).hasValue("foo");
  }

  @Test
  public void retryUnmocked() {
    var mockedConfig = mockedConfig(3, Integer.MAX_VALUE, 1, 1000, 1);

    var clock = mockedClock(3);
    var tryLoopState = new RetryLoopImpl<String>(mockedConfig, clock);

    var retryCounter = new AtomicInteger();
    var result = new AtomicReference<String>();

    soft.assertThatCode(
            () ->
                result.set(
                    tryLoopState.retryLoop(
                        (long nanosRemaining) -> {
                          if (retryCounter.incrementAndGet() == 1) {
                            return Optional.empty();
                          }
                          return Optional.of("foo");
                        })))
        .doesNotThrowAnyException();

    soft.assertThat(retryCounter).hasValue(2);
    soft.assertThat(result).hasValue("foo");
  }

  @Test
  public void sleepConsidersAttemptDuration() {
    var mockedConfig =
        mockedConfig(Integer.MAX_VALUE, Integer.MAX_VALUE, 100, 100, Integer.MAX_VALUE);

    var clock = mockedClock(3);
    var tryLoopState = new RetryLoopImpl<>(mockedConfig, clock);
    var t0 = clock.nanoTime();

    soft.assertThat(tryLoopState.canRetry(t0, MILLISECONDS.toNanos(20))).isTrue();
    verify(clock, times(1)).sleepMillis(80L);

    // bounds doubled

    soft.assertThat(tryLoopState.canRetry(t0, MILLISECONDS.toNanos(30))).isTrue();
    verify(clock, times(1)).sleepMillis(170L);
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 5, 50, 100, 200})
  public void doesNotSleepLongerThanMax(long maxSleep) {
    var retries = 50;
    var clock = mockedClock(retries);

    var initialLower = 1L;
    var initialUpper = 2L;

    var lower = initialLower;
    var upper = initialUpper;
    var tryLoopState =
        new RetryLoopImpl<>(mockedConfig(retries, Integer.MAX_VALUE, 1, upper, maxSleep), clock);
    var t0 = clock.nanoTime();

    verify(clock, times(1)).nanoTime();

    var inOrderClock = inOrder(clock);

    for (int i = 0; i < retries; i++) {
      soft.assertThat(tryLoopState.canRetry(t0, 0L)).isTrue();
      long finalLower = lower;
      long finalUpper = upper;
      ArgumentMatcher<Long> matcher =
          new ArgumentMatcher<>() {
            @Override
            public boolean matches(Long l) {
              return l >= finalLower && l <= finalUpper && l <= maxSleep;
            }

            @Override
            public String toString() {
              return "lower = " + finalLower + ", upper = " + finalUpper + ", max = " + maxSleep;
            }
          };
      inOrderClock.verify(clock, times(1)).sleepMillis(longThat(matcher));

      if (upper * 2 <= maxSleep) {
        lower *= 2;
        upper *= 2;
      } else {
        upper = maxSleep;
      }
    }

    verify(clock, times(1 + retries)).nanoTime();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  public void retriesWithinBounds(int retries) {
    var clock = mockedClock(retries);

    var tryLoopState = new RetryLoopImpl<>(mockedConfig(retries, 42L), clock);
    var t0 = clock.nanoTime();

    verify(clock, times(1)).nanoTime();

    for (var i = 0; i < retries; i++) {
      soft.assertThat(tryLoopState.canRetry(t0, 0L)).isTrue();
    }

    verify(clock, times(1 + retries)).nanoTime();
    verify(clock, times(retries)).sleepMillis(anyLong());
  }

  @Test
  public void retryUnsuccessful() {
    var retries = 3;

    var clock = mockedClock(retries);

    var tryLoopState = new RetryLoopImpl<>(mockedConfig(retries, 42L), clock);
    var t0 = clock.nanoTime();

    for (var i = 0; i < retries; i++) {
      soft.assertThat(tryLoopState.canRetry(t0, 0L)).isTrue();
    }

    soft.assertThat(tryLoopState.canRetry(t0, 0L)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  public void retriesOutOfBounds(int retries) {
    var clock = mockedClock(retries);

    var tryLoopState = new RetryLoopImpl<>(mockedConfig(retries - 1, 42L), clock);
    var t0 = clock.nanoTime();

    verify(clock, times(1)).nanoTime();

    for (var i = 0; i < retries - 1; i++) {
      soft.assertThat(tryLoopState.canRetry(t0, 0L)).isTrue();
    }

    verify(clock, times(retries)).nanoTime();
    verify(clock, times(retries - 1)).sleepMillis(anyLong());

    soft.assertThat(tryLoopState.canRetry(t0, 0L)).isFalse();
  }

  @Test
  public void sleepDurations() {
    var retries = 10;

    var clock = mockedClock(retries);

    // Must be "big" enough so that the upper/lower sleep-time-bounds doubling exceed this value
    var timeoutMillis = 42L;

    var config = mockedConfig(retries, timeoutMillis);
    var tryLoopState = new RetryLoopImpl<>(config, clock);
    var t0 = clock.nanoTime();

    var lower = config.initialSleepLower().toMillis();
    var upper = config.initialSleepUpper().toMillis();

    for (var i = 0; i < retries; i++) {
      soft.assertThat(tryLoopState.canRetry(t0, 0L)).isTrue();

      long l = Math.min(lower, timeoutMillis);
      long u = Math.min(upper, timeoutMillis);

      verify(clock).sleepMillis(longThat(v -> v >= l && v <= u));
      clearInvocations(clock);

      lower *= 2;
      upper *= 2;
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 50})
  public void retriesOutOfTime(int retries) {
    var times = new Long[retries];
    Arrays.fill(times, 0L);
    times[retries - 1] = MILLISECONDS.toNanos(43L);
    var clock = mockedClock(0L, times);

    var tryLoopState = new RetryLoopImpl<>(mockedConfig(retries, 42L), clock);
    var t0 = clock.nanoTime();

    verify(clock, times(1)).nanoTime();

    for (var i = 0; i < retries - 1; i++) {
      soft.assertThat(tryLoopState.canRetry(t0, 0L)).isTrue();
    }

    verify(clock, times(retries)).nanoTime();
    verify(clock, times(retries - 1)).sleepMillis(anyLong());

    soft.assertThat(tryLoopState.canRetry(t0, 0L)).isFalse();

    // Trigger the `if (unsuccessful)` case in TryLoopState.retry
    soft.assertThat(tryLoopState.canRetry(t0, 0L)).isFalse();
  }

  MonotonicClock mockedClock(int retries) {
    var times = new Long[retries];
    Arrays.fill(times, 0L);
    return mockedClock(0L, times);
  }

  MonotonicClock mockedClock(Long t0, Long... times) {
    var mock = spy(MonotonicClock.class);
    when(mock.nanoTime()).thenReturn(t0, times);
    doNothing().when(mock).sleepMillis(anyLong());
    return mock;
  }

  RetryConfig mockedConfig(int retries, long commitTimeout) {
    return mockedConfig(retries, commitTimeout, 5L, 25L, Integer.MAX_VALUE);
  }

  RetryConfig mockedConfig(
      int commitRetries, long commitTimeout, long lowerDefault, long upperDefault, long maxSleep) {
    var mock = mock(RetryConfig.class);
    when(mock.retries()).thenReturn(commitRetries);
    when(mock.timeout()).thenReturn(Duration.ofMillis(commitTimeout));
    when(mock.initialSleepLower()).thenReturn(Duration.ofMillis(lowerDefault));
    when(mock.initialSleepUpper()).thenReturn(Duration.ofMillis(upperDefault));
    when(mock.maxSleep()).thenReturn(Duration.ofMillis(maxSleep));
    when(mock.fairRetries()).thenReturn(FairRetriesType.UNFAIR);
    return mock;
  }
}
