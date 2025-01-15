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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class TestProcessHandler {
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
    executor.awaitTermination(10, SECONDS);
  }

  @Test
  void notStarted() {
    var phMock = new ProcessHandlerMock();

    soft.assertThatThrownBy(phMock.ph::stop)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("No process started");
  }

  @Test
  void doubleStart() {
    var phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    soft.assertThatThrownBy(() -> phMock.ph.started(phMock.proc))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Process already started");
  }

  @RepeatedTest(20)
  // repeat, risk of flakiness
  void processWithNoOutput() {
    var phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    var futureListenUrl = executor.submit(phMock.ph::getListenUrls);

    while (phMock.clock.get() < TimeUnit.MILLISECONDS.toNanos(phMock.timeToUrl)) {
      soft.assertThat(futureListenUrl).isNotDone();
      soft.assertAll();
      phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    }
    // should be exactly at (but not "past") the time to wait for the listen-url now

    // bump the clock "past" the listen-url-timeout
    phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));

    soft.assertThat(futureListenUrl)
        .failsWithin(5, SECONDS)
        .withThrowableOfType(ExecutionException.class) // EE from ForkJoinPool/executor (test code)
        .withRootCauseInstanceOf(
            TimeoutException.class) // TE from ProcessHandler/ListenUrlWaiter.getListenUrl
        .withMessageEndingWith(ListenUrlWaiter.TIMEOUT_MESSAGE + ListenUrlWaiter.NOTHING_RECEIVED);

    // Need to wait for the watchdog to finish, before we can do any further assertion
    phMock.ph.watchdogExitGrace();

    soft.assertThat(phMock.ph.isAlive()).isFalse();
  }

  @RepeatedTest(20)
  // repeat, risk of flakiness
  void processExitsEarly() {
    var phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    var futureListenUrl = executor.submit(phMock.ph::getListenUrls);

    soft.assertThat(phMock.ph.isAlive()).isTrue();
    soft.assertThatThrownBy(() -> phMock.ph.getExitCode())
        .isInstanceOf(IllegalThreadStateException.class);

    phMock.exitCode.set(88);

    soft.assertThat(futureListenUrl)
        .failsWithin(5, SECONDS)
        .withThrowableOfType(ExecutionException.class) // EE from ForkJoinPool/executor (test code)
        .withMessageEndingWith(
            ListenUrlWaiter.TIMEOUT_MESSAGE
                + " Process exited early, exit code is 88."
                + ListenUrlWaiter.NOTHING_RECEIVED);

    // Need to wait for the watchdog to finish, before we can do any further assertion
    phMock.ph.watchdogExitGrace();

    soft.assertThat(phMock.ph.isAlive()).isFalse();
    soft.assertThat(phMock.ph.getExitCode()).isEqualTo(88);
  }

  @RepeatedTest(20)
  // repeat, risk of flakiness
  void processLotsOfIoNoListen() throws Exception {
    var phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    var futureListenUrl = executor.submit(phMock.ph::getListenUrls);

    var stdoutMessage = "Hello world\n";
    var message = stdoutMessage.toCharArray();
    while (phMock.clock.get() < TimeUnit.MILLISECONDS.toNanos(phMock.timeToUrl)) {
      for (var c : message) {
        phMock.stdout.add((byte) c);
      }
      soft.assertThat(futureListenUrl).isNotDone();
      soft.assertAll();
      phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    }
    // should be exactly at (but not "past") the time to wait for the listen-url now

    soft.assertThat(phMock.ph.remainingWaitTimeNanos()).isEqualTo(0);
    soft.assertThat(phMock.ph.isAlive()).isTrue();

    var timeoutFail = System.currentTimeMillis() + SECONDS.toMillis(10);
    while (!phMock.stdout.isEmpty()) {
      soft.assertThat(System.currentTimeMillis() < timeoutFail).isTrue();
      soft.assertAll();
      Thread.sleep(1L);
    }

    // bump the clock "past" the listen-url-timeout
    phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));

    soft.assertThat(futureListenUrl)
        .failsWithin(5, SECONDS)
        .withThrowableOfType(ExecutionException.class) // EE from ForkJoinPool/executor (test code)
        .withRootCauseInstanceOf(
            TimeoutException.class) // TE from ProcessHandler/ListenUrlWaiter.getListenUrl
        .withMessageContaining(
            ListenUrlWaiter.TIMEOUT_MESSAGE + ListenUrlWaiter.CAPTURED_LOG_FOLLOWS)
        .withMessageContaining(stdoutMessage);

    // Need to wait for the watchdog to finish, before we can do any further assertion
    phMock.ph.watchdogExitGrace();

    soft.assertThat(phMock.ph.isAlive()).isFalse();
    soft.assertThat(phMock.ph.getExitCode()).isGreaterThanOrEqualTo(0);

    soft.assertThat(phMock.stdoutLines).hasSize((int) (phMock.timeToUrl / 10));
  }

  @RepeatedTest(20)
  // repeat, risk of flakiness
  void processLotsOfIoProperListenUrl() {
    var phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    var futureListenUrl = executor.submit(phMock.ph::getListenUrls);

    while (phMock.clock.get() < TimeUnit.MILLISECONDS.toNanos(phMock.timeToUrl / 2)) {
      for (var c : "Hello world\n".toCharArray()) {
        phMock.stdout.add((byte) c);
      }
      soft.assertThat(futureListenUrl).isNotDone();
      soft.assertAll();
      phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    }
    // should be exactly at (but not "past") the time to wait for the listen-url now

    for (var c : "Quarkus startup message... Listening on: http://0.0.0.0:4242\n".toCharArray()) {
      phMock.stdout.add((byte) c);
    }

    // bump the clock "past" the listen-url-timeout
    phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));

    soft.assertThat(futureListenUrl)
        .succeedsWithin(5, SECONDS)
        .isEqualTo(Arrays.asList("http://0.0.0.0:4242", null));

    soft.assertThat(phMock.ph.isAlive()).isTrue();
    soft.assertThatThrownBy(() -> phMock.ph.getExitCode())
        .isInstanceOf(IllegalThreadStateException.class);

    // The .stop() waits until the watchdog has finished its work
    phMock.ph.stop();

    soft.assertThat(phMock.ph.isAlive()).isFalse();
    soft.assertThat(phMock.ph.getExitCode()).isGreaterThanOrEqualTo(0);

    soft.assertThat(phMock.stdoutLines).hasSize((int) (phMock.timeToUrl / 10 / 2) + 1);
  }

  static final class ProcessHandlerMock {

    AtomicLong clock = new AtomicLong();

    AtomicInteger exitCode = new AtomicInteger(-1);

    // Full lines received "form the process" via stdout/stderr is collected in these lists
    List<String> stdoutLines = Collections.synchronizedList(new ArrayList<>());

    // Data that's "written by the process" to stdout/stderr is "piped" through these queues
    ArrayBlockingQueue<Byte> stdout = new ArrayBlockingQueue<>(1024);

    @SuppressWarnings("InputStreamSlowMultibyteRead")
    InputStream stdoutStream =
        new InputStream() {
          @Override
          public int available() {
            return stdout.size();
          }

          @Override
          public int read() {
            var b = stdout.poll();
            return b == null ? -1 : b.intValue();
          }
        };

    Process proc =
        new Process() {
          @Override
          public OutputStream getOutputStream() {
            throw new UnsupportedOperationException();
          }

          @Override
          public InputStream getInputStream() {
            return stdoutStream;
          }

          @Override
          public InputStream getErrorStream() {
            return stdoutStream;
          }

          @Override
          public int waitFor() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
            return super.waitFor(timeout, unit);
          }

          @Override
          public int exitValue() {
            var ec = exitCode.get();
            if (ec < 0) {
              throw new IllegalThreadStateException();
            }
            return ec;
          }

          @Override
          public void destroy() {
            exitCode.set(42);
          }

          @Override
          public Process destroyForcibly() {
            exitCode.set(42);
            return this;
          }
        };

    long timeToUrl = 500;

    ProcessHandler ph =
        new ProcessHandler()
            .setStdoutTarget(stdoutLines::add)
            .setTicker(clock::get)
            .setTimeToListenUrlMillis(timeToUrl)
            .setTimeStopMillis(42);
  }
}
