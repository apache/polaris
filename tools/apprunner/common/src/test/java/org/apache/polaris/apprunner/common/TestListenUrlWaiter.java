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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class TestListenUrlWaiter {
  @InjectSoftAssertions SoftAssertions soft;

  private static ExecutorService executor;

  @BeforeAll
  static void createExecutor() {
    executor = Executors.newCachedThreadPool();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @AfterAll
  static void stopExecutor() throws Exception {
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
  }

  @Test
  void ioHandling() {
    var clock = new AtomicLong();
    var timeout = 10L;

    var line = new AtomicReference<>();
    var waiter = new ListenUrlWaiter(clock::get, timeout, line::set);

    waiter.accept("Hello World");
    soft.assertThat(line.getAndSet(null)).isEqualTo("Hello World");
    soft.assertThat(waiter.peekListenUrls()).isNull();

    waiter.accept("");
    soft.assertThat(line.getAndSet(null)).isEqualTo("");
    soft.assertThat(waiter.peekListenUrls()).isNull();

    var listenLine =
        "2021-05-28 12:12:25,753 INFO  [io.quarkus] (main) nessie-quarkus 0.6.2-SNAPSHOT on JVM (powered by Quarkus 1.13.4.Final) started in 1.444s. Listening on: http://0.0.0.0:39423. Management interface listening on http://0.0.0.0:9000.";
    waiter.accept(listenLine);
    soft.assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    soft.assertThat(waiter.peekListenUrls())
        .containsExactly("http://0.0.0.0:39423", "http://0.0.0.0:9000");

    // Must *not* change the already extracted listen-url
    listenLine =
        "2021-05-28 12:12:25,753 INFO  [io.quarkus] (main) nessie-quarkus 0.6.2-SNAPSHOT on JVM (powered by Quarkus 1.13.4.Final) started in 1.444s. Listening on: http://4.2.4.2:4242";
    waiter.accept(listenLine);
    soft.assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    soft.assertThat(waiter.peekListenUrls())
        .containsExactly("http://0.0.0.0:39423", "http://0.0.0.0:9000");

    waiter = new ListenUrlWaiter(clock::get, timeout, line::set);
    listenLine =
        "2021-05-28 12:12:25,753 INFO  [io.quarkus] (main) nessie-quarkus 0.6.2-SNAPSHOT on JVM (powered by Quarkus 1.13.4.Final) started in 1.444s. Listening on: https://localhost.in.some.space:12345";
    waiter.accept(listenLine);
    soft.assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    soft.assertThat(waiter.peekListenUrls())
        .containsExactly("https://localhost.in.some.space:12345", null);

    waiter = new ListenUrlWaiter(clock::get, timeout, line::set);
    listenLine = "Listening on: https://localhost.in.some.space:4242";
    waiter.accept(listenLine);
    soft.assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    soft.assertThat(waiter.peekListenUrls())
        .containsExactly("https://localhost.in.some.space:4242", null);
  }

  @RepeatedTest(20) // repeat, risk of flakiness
  void timeout() {
    var clock = new AtomicLong();
    var timeout = 10_000L; // long timeout, for slow CI

    var line = new AtomicReference<>();
    var waiter = new ListenUrlWaiter(clock::get, timeout, line::set);

    soft.assertThat(waiter.isTimeout()).isFalse();

    clock.set(TimeUnit.MILLISECONDS.toNanos(timeout + 1));

    soft.assertThat(waiter.isTimeout()).isTrue();

    soft.assertThat(executor.submit(waiter::getListenUrls))
        .failsWithin(5, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class)
        .withRootCauseExactlyInstanceOf(TimeoutException.class)
        .withMessageEndingWith(ListenUrlWaiter.TIMEOUT_MESSAGE + ListenUrlWaiter.NOTHING_RECEIVED);
  }

  @RepeatedTest(20) // repeat, risk of flakiness
  void noTimeout() throws Exception {
    var clock = new AtomicLong();
    var timeout = 10_000L; // long timeout, for slow CI

    // Note: the implementation uses "our clock" to check the timeout, but uses a "standard
    // Future.get(time)" for the actual get.

    var line = new AtomicReference<>();
    var waiter = new ListenUrlWaiter(clock::get, timeout, line::set);

    // Clock exactly at the timeout-boundary is not a timeout
    clock.set(TimeUnit.MILLISECONDS.toNanos(timeout));

    var listenLine =
        "2021-05-28 12:12:25,753 INFO  [io.quarkus] (main) nessie-quarkus 0.6.2-SNAPSHOT on JVM (powered by Quarkus 1.13.4.Final) started in 1.444s. Listening on: http://4.2.4.2:4242. Management interface listening on http://4.2.4.2:2424.";
    waiter.accept(listenLine);
    soft.assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    soft.assertThat(waiter.getListenUrls())
        .containsExactly("http://4.2.4.2:4242", "http://4.2.4.2:2424");
    soft.assertThat(waiter.isTimeout()).isFalse();

    // Clock post the timeout-boundary (so a timeout-check would trigger)
    clock.set(TimeUnit.MILLISECONDS.toNanos(timeout + 1));
    soft.assertThat(waiter.getListenUrls())
        .containsExactly("http://4.2.4.2:4242", "http://4.2.4.2:2424");
    soft.assertThat(waiter.isTimeout()).isFalse();
  }
}
