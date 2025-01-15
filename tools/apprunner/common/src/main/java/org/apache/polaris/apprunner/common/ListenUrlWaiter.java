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
package org.apache.polaris.apprunner.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * Accepts {@link String}s via it's {@link #accept(String)} method and checks for the {@code
 * Listening on: http...} pattern.
 */
final class ListenUrlWaiter implements Consumer<String> {

  private static final Pattern HTTP_PORT_LOG_PATTERN =
      Pattern.compile(
          "^.*Listening on: (http[s]?://[^ ]*)([.] Management interface listening on (http[s]?://[^ ]*)[.])?$");
  static final String TIMEOUT_MESSAGE =
      "Did not get the http(s) listen URL from the console output.";
  private static final long MAX_ITER_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
  public static final String NOTHING_RECEIVED = " No output received from process.";
  public static final String CAPTURED_LOG_FOLLOWS = " Captured output follows:\n";

  private final LongSupplier clock;
  private final Consumer<String> stdoutTarget;
  private final long deadlineListenUrl;

  private final CompletableFuture<List<String>> listenUrl = new CompletableFuture<>();
  private final List<String> capturedLog = new ArrayList<>();

  /**
   * Construct a new instance to wait for Quarkus' {@code Listening on: ...} message.
   *
   * @param clock monotonic clock, nanoseconds
   * @param timeToListenUrlMillis timeout in millis, the "Listen on: ..." must be received within
   *     this time (otherwise it will fail)
   * @param stdoutTarget "real" target for "stdout"
   */
  ListenUrlWaiter(LongSupplier clock, long timeToListenUrlMillis, Consumer<String> stdoutTarget) {
    this.clock = clock;
    this.stdoutTarget = stdoutTarget;
    this.deadlineListenUrl =
        clock.getAsLong() + TimeUnit.MILLISECONDS.toNanos(timeToListenUrlMillis);
  }

  @Override
  public void accept(String line) {
    if (!listenUrl.isDone()) {
      synchronized (capturedLog) {
        capturedLog.add(line);
        var m = HTTP_PORT_LOG_PATTERN.matcher(line);
        if (m.matches()) {
          listenUrl.complete(Arrays.asList(m.group(1), m.group(3)));
          capturedLog.clear();
        }
      }
    }
    stdoutTarget.accept(line);
  }

  List<String> peekListenUrls() {
    try {
      return listenUrl.isDone() ? listenUrl.get() : null;
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }

  /**
   * Get the first captured {@code Listening on: http...} pattern.
   *
   * @return the captured listen URL or {@code null} if none has been found (so far).
   */
  List<String> getListenUrls() throws InterruptedException, TimeoutException {
    while (true) {
      var remainingNanos = remainingNanos();
      // must succeed if the listen-url has been captured, even if it's called after the timeout has
      // elapsed
      if (remainingNanos < 0 && !listenUrl.isDone()) {
        throw getTimeoutException(null);
      }

      try {
        return listenUrl.get(Math.min(MAX_ITER_WAIT_NANOS, remainingNanos), TimeUnit.NANOSECONDS);
      } catch (TimeoutException e) {
        // Continue, check above.
        // This "short get()" is implemented to make the unit test TestListenUrlWaiter.noTimeout()
        // run faster.
      } catch (ExecutionException e) {
        if (e.getCause() instanceof TimeoutException) {
          throw getTimeoutException(e.getCause());
        }
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          throw new RuntimeException(e.getCause());
        }
      }
    }
  }

  private TimeoutException getTimeoutException(Throwable cause) {
    String log;
    synchronized (capturedLog) {
      log = String.join("\n", capturedLog);
    }
    var ex =
        new TimeoutException(
            TIMEOUT_MESSAGE + (log.isEmpty() ? NOTHING_RECEIVED : (CAPTURED_LOG_FOLLOWS + log)));
    if (cause != null) {
      ex.addSuppressed(cause);
    }
    return ex;
  }

  void stopped(String reason) {
    listenUrl.completeExceptionally(new RuntimeException(reason));
  }

  void timedOut() {
    listenUrl.completeExceptionally(new TimeoutException());
  }

  public void exited(int exitCode) {
    // No-op, if the listen-URL has already been received, so using the TIMEOUT_MESSAGE here is
    // fine.
    String log;
    synchronized (capturedLog) {
      log = String.join("\n", capturedLog);
    }
    listenUrl.completeExceptionally(
        new RuntimeException(
            ListenUrlWaiter.TIMEOUT_MESSAGE
                + " Process exited early, exit code is "
                + exitCode
                + "."
                + (log.isEmpty() ? NOTHING_RECEIVED : (CAPTURED_LOG_FOLLOWS + log))));
  }

  long remainingNanos() {
    return deadlineListenUrl - clock.getAsLong();
  }

  boolean isTimeout() {
    if (listenUrl.isDone() && !listenUrl.isCompletedExceptionally()) {
      return false;
    }
    return remainingNanos() < 0;
  }
}
