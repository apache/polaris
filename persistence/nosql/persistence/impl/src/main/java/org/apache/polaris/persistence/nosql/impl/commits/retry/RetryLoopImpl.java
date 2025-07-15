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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.polaris.persistence.nosql.impl.commits.retry.RetryStatsConsumer.Result.CONFLICT;
import static org.apache.polaris.persistence.nosql.impl.commits.retry.RetryStatsConsumer.Result.ERROR;
import static org.apache.polaris.persistence.nosql.impl.commits.retry.RetryStatsConsumer.Result.SUCCESS;
import static org.apache.polaris.persistence.nosql.impl.commits.retry.RetryStatsConsumer.Result.TIMEOUT;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig;
import org.apache.polaris.persistence.nosql.api.commit.RetryTimeoutException;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;

final class RetryLoopImpl<RESULT> implements RetryLoop<RESULT> {

  private final FairRetries fairRetries;
  private final MonotonicClock monotonicClock;
  private final long maxTime;
  private final int maxRetries;
  private final long maxSleep;
  private long lowerBound;
  private long upperBound;
  private int retries;
  private long sleepTime;
  private RetryStatsConsumer retryStatsConsumer;

  RetryLoopImpl(RetryConfig config, MonotonicClock monotonicClock) {
    this.maxTime = config.timeout().toNanos();
    this.maxRetries = config.retries();
    this.monotonicClock = monotonicClock;
    this.lowerBound = config.initialSleepLower().toMillis();
    this.upperBound = config.initialSleepUpper().toMillis();
    this.maxSleep = config.maxSleep().toMillis();
    this.fairRetries = FairRetries.create(config.fairRetries());
  }

  @Override
  public RetryLoop<RESULT> setRetryStatsConsumer(RetryStatsConsumer retryStatsConsumer) {
    this.retryStatsConsumer = retryStatsConsumer;
    return this;
  }

  @Override
  public RESULT retryLoop(Retryable<RESULT> retryable)
      throws CommitException, RetryTimeoutException {
    var timeLoopStarted = currentNanos();
    var timeoutAt = timeLoopStarted + maxTime;
    var timeAttemptStarted = timeLoopStarted;
    var prio = -1; // -1 means not acquired
    try {
      for (var attempt = 0; true; attempt++, timeAttemptStarted = currentNanos()) {
        prio = fairRetries.beforeAttempt(attempt, prio);
        try {
          var r = retryable.attempt(timeoutAt - timeAttemptStarted);
          if (r.isPresent()) {
            reportEnd(SUCCESS, timeAttemptStarted);
            return r.get();
          }
          retryOrFail(timeLoopStarted, timeAttemptStarted, attempt);
        } catch (UnknownOperationResultException e) {
          retryOrFail(timeLoopStarted, timeAttemptStarted, attempt);
        }
      }
    } catch (CommitException e) {
      reportEnd(CONFLICT, timeAttemptStarted);
      throw e;
    } catch (RuntimeException e) {
      reportEnd(ERROR, timeAttemptStarted);
      throw e;
    } finally {
      if (prio != -1) {
        fairRetries.done(prio);
      }
    }
  }

  private void reportEnd(RetryStatsConsumer.Result result, long timeAttemptStarted) {
    var c = retryStatsConsumer;
    if (c != null) {
      c.retryLoopFinished(result, retries, sleepTime, currentNanos() - timeAttemptStarted);
    }
  }

  private void retryOrFail(long timeLoopStarted, long timeAttemptStarted, int attempt)
      throws RetryTimeoutException {
    if (canRetry(timeLoopStarted, timeAttemptStarted)) {
      return;
    }
    reportEnd(TIMEOUT, timeAttemptStarted);
    throw new RetryTimeoutException(attempt, currentNanos() - timeLoopStarted);
  }

  long currentNanos() {
    return monotonicClock.nanoTime();
  }

  boolean canRetry(long timeLoopStarted, long timeAttemptStarted) {
    retries++;

    var current = currentNanos();
    var totalElapsed = current - timeLoopStarted;
    var attemptElapsed = timeAttemptStarted - current;

    if (maxTime < totalElapsed || maxRetries < retries) {
      return false;
    }

    sleepAndBackoff(totalElapsed, attemptElapsed);

    return true;
  }

  private void sleepAndBackoff(long totalElapsed, long attemptElapsed) {
    var lower = lowerBound;
    var upper = upperBound;
    var sleepMillis = lower == upper ? lower : ThreadLocalRandom.current().nextLong(lower, upper);

    // Prevent that we "sleep" too long and exceed 'maxTime'
    sleepMillis = Math.min(NANOSECONDS.toMillis(Math.max(0, maxTime - totalElapsed)), sleepMillis);

    // consider the already elapsed time of the last attempt
    sleepMillis = Math.max(1L, sleepMillis - NANOSECONDS.toMillis(attemptElapsed));

    sleepTime += sleepMillis;
    monotonicClock.sleepMillis(sleepMillis);

    upper = upper * 2;
    long max = maxSleep;
    if (upper <= max) {
      lowerBound *= 2;
      upperBound = upper;
    } else {
      upperBound = max;
    }
  }
}
