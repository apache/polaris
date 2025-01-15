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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Handles the execution of an external process, focused on running a Quarkus application jar.
 *
 * <p>Starts the process configured in a {@link ProcessBuilder}, provides a method to get the {@link
 * #getListenUrls() Quarkus HTTP listen URL} as Quarkus prints to stdout, and manages process
 * lifetime and line-by-line I/O pass-through for stdout + stderr.
 *
 * <p>Any instance of this class can only be used to start (and stop) one process and cannot be
 * reused for another process.
 *
 * <p>This implementation is not thread-safe.
 */
public class ProcessHandler {

  // intentionally long timeouts - think: slow CI systems
  public static final long MILLIS_TO_HTTP_PORT = 30_000L;
  public static final long MILLIS_TO_STOP = 15_000L;

  private LongSupplier ticker = System::nanoTime;

  private static final int NOT_STARTED = -1;
  private static final int RUNNING = -2;
  private static final int ERROR = -3;
  private final AtomicInteger exitCode = new AtomicInteger(NOT_STARTED);

  private final AtomicBoolean stopped = new AtomicBoolean();

  private Process process;

  private long timeToListenUrlMillis = MILLIS_TO_HTTP_PORT;
  private long timeStopMillis = MILLIS_TO_STOP;

  private Consumer<String> stdoutTarget = System.out::println;
  private ListenUrlWaiter listenUrlWaiter;

  private volatile ExecutorService watchdogExecutor;
  private volatile Future<?> watchdogFuture;
  private volatile Thread shutdownHook;

  public ProcessHandler() {
    // empty
  }

  public ProcessHandler setTimeToListenUrlMillis(long timeToListenUrlMillis) {
    this.timeToListenUrlMillis = timeToListenUrlMillis;
    return this;
  }

  public ProcessHandler setTimeStopMillis(long timeStopMillis) {
    this.timeStopMillis = timeStopMillis;
    return this;
  }

  public ProcessHandler setStdoutTarget(Consumer<String> stdoutTarget) {
    this.stdoutTarget = stdoutTarget;
    return this;
  }

  public ProcessHandler setTicker(LongSupplier ticker) {
    this.ticker = ticker;
    return this;
  }

  /**
   * Starts the process from the given {@link ProcessBuilder}.
   *
   * @param processBuilder process to start
   * @return instance handling the process' runtime
   * @throws IOException usually, if the process fails to start
   */
  public ProcessHandler start(ProcessBuilder processBuilder) throws IOException {
    if (process != null) {
      throw new IllegalStateException("Process already started");
    }

    return started(processBuilder.redirectErrorStream(true).start());
  }

  /**
   * Alternative to {@link #start(ProcessBuilder)}, directly configures a running process.
   *
   * @param process running process
   * @return {@code this}
   */
  ProcessHandler started(Process process) {
    if (this.process != null) {
      throw new IllegalStateException("Process already started");
    }

    listenUrlWaiter = new ListenUrlWaiter(ticker, timeToListenUrlMillis, stdoutTarget);

    this.process = process;
    exitCode.set(RUNNING);

    shutdownHook = new Thread(this::shutdownHandler);
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    watchdogExecutor = Executors.newSingleThreadExecutor();
    watchdogFuture = watchdogExecutor.submit(this::watchdog);

    return this;
  }

  /**
   * Returns the http(s) listen URL as a string as emitted to stdout by Quarkus.
   *
   * <p>If the Quarkus process does not emit that URL within the time configured via {@link
   * #setTimeToListenUrlMillis(long)}, which defaults to {@value #MILLIS_TO_HTTP_PORT} ms, this
   * method will throw an {@link IllegalStateException}.
   *
   * @return the listen URL, never {@code null}.
   * @throws InterruptedException if the current thread was interrupted while waiting for the listen
   *     URL.
   * @throws TimeoutException if the Quarkus process did not write the listen URL to stdout.
   */
  public List<String> getListenUrls() throws InterruptedException, TimeoutException {
    return listenUrlWaiter.getListenUrls();
  }

  /**
   * Stops the process.
   *
   * <p>Tries to gracefully stop the process via a {@code SIGTERM}. If the process is still alive
   * after {@link #setTimeStopMillis(long)}, which defaults to {@value #MILLIS_TO_STOP} ms, the
   * process will be killed with a {@code SIGKILL}.
   */
  public void stop() {
    if (process == null) {
      throw new IllegalStateException("No process started");
    }

    doStop("Stopped by plugin");

    watchdogExitGrace();
  }

  private void shutdownHandler() {
    doStop("Stop by shutdown handler");
  }

  private void doStop(String reason) {
    if (stopped.compareAndSet(false, true)) {
      try {
        if (reason != null) {
          listenUrlWaiter.stopped(reason);
        } else {
          listenUrlWaiter.timedOut();
        }
        process.destroy();
        try {
          if (!process.waitFor(timeStopMillis, TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
          }
        } catch (InterruptedException e) {
          process.destroyForcibly();
          Thread.currentThread().interrupt();
        }
        watchdogExecutor.shutdown();
      } finally {
        try {
          // Don't remove the shutdown-hook if we're running in the shutdown-hook
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
          // ignore (might happen, when a JVM shutdown is already in progress)
        }
      }
    }
  }

  void watchdogExitGrace() {
    try {
      // Give the watchdog task/thread some time to finish its work
      watchdogFuture.get(timeStopMillis, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw new RuntimeException("ProcessHandler's watchdog thread failed.", e);
    } catch (TimeoutException e) {
      throw new IllegalStateException("ProcessHandler's watchdog thread failed to finish in time.");
    } catch (InterruptedException e) {
      process.destroyForcibly();
      Thread.currentThread().interrupt();
    }
  }

  public boolean isAlive() {
    return exitCode.get() == RUNNING;
  }

  /**
   * Retrieves the exit-code of the process, if it terminated or throws a {@link
   * IllegalThreadStateException} if it is still alive.
   *
   * @return the exit code of the process
   * @throws IllegalThreadStateException if the process is still alive
   */
  public int getExitCode() throws IllegalThreadStateException {
    if (isAlive()) {
      throw new IllegalThreadStateException();
    }
    return exitCode.get();
  }

  long remainingWaitTimeNanos() {
    return listenUrlWaiter.remainingNanos();
  }

  private Object watchdog() throws IOException {
    try (var out = process.getInputStream()) {
      var stdout = new InputBuffer(out, listenUrlWaiter);
      try {

        /*
         * I/O loop.
         *
         * Fetches data from stdout + stderr and pushes the read data to the associated `InputBuffer`
         * instances. The one for `stdout` listens for the HTTP listen address from Quarkus.
         *
         * As long as there is data from stdout or stderr, the loop does not wait/sleep to get data
         * out as fast as possible. If there's no data available, the loop will "yield" via a
         * Thread.sleep(1L), which is good enough.
         *
         * Note: we cannot do blocking-I/O here, because we have to read from both stdout+stderr.
         *
         * If the process exits, the loop will exit as soon as there is no more data left from
         * stdout/stderr.
         */
        while (true) {
          var anyIo = stdout.io();

          try {
            var ec = process.exitValue();
            exitCode.set(ec);
            if (!anyIo) {
              listenUrlWaiter.exited(exitCode.get());
              break;
            }
          } catch (IllegalThreadStateException e) {
            // server still alive
          }

          if (listenUrlWaiter.isTimeout() && !stopped.get()) {
            doStop(null);
          }

          if (!anyIo) {
            try {
              // Yield CPU for a little while, so this background thread does not consume 100% CPU.
              Thread.sleep(1L);
            } catch (InterruptedException interruptedException) {
              doStop("ProcessHandler's watchdog thread interrupted.");
              exitCode.set(ERROR);
              break;
            }
          }
        }
      } finally {
        stdout.flush();
      }
    }
    return null;
  }
}
