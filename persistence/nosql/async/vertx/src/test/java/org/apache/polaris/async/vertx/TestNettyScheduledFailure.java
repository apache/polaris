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

package org.apache.polaris.async.vertx;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
@Disabled(
    "Reproducer for https://github.com/netty/netty/issues/15492 - eventually remove this code")
public class TestNettyScheduledFailure {
  @InjectSoftAssertions SoftAssertions soft;

  Vertx vertx;
  EventLoop eventLoop;
  final Duration asyncTimeout = Duration.ofMinutes(5);
  NioEventLoopGroup eventLoopGroup;

  @BeforeEach
  public void before() {
    vertx =
        Vertx.vertx(
            new VertxOptions()
                .setInternalBlockingPoolSize(16)
                .setEventLoopPoolSize(16)
                .setWorkerPoolSize(32));

    eventLoopGroup =
        new NioEventLoopGroup(
            32,
            r -> {
              return new Thread(r);
            });
    eventLoop = eventLoopGroup.next();
  }

  @AfterEach
  public void after() {
    vertx.close();
    eventLoop.close();
    eventLoopGroup.close();
  }

  @Test
  public void reproNettyOnly() throws Exception {
    var threads = 32;
    try (var exec = Executors.newFixedThreadPool(threads)) {
      for (int i = 0; i < 10000; i++) {
        CompletableFuture.allOf(
                IntStream.range(0, threads)
                    .mapToObj(
                        x ->
                            CompletableFuture.runAsync(
                                () -> {
                                  for (int i1 = 0; i1 < 2; i1++) {
                                    var sem = new Semaphore(0);

                                    var scheduledFuture =
                                        eventLoop.scheduleAtFixedRate(
                                            sem::release, 1, 1, MILLISECONDS);

                                    try {
                                      soft.assertThat(
                                              sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
                                          .isTrue();
                                    } catch (InterruptedException e) {
                                      throw new RuntimeException(e);
                                    }

                                    scheduledFuture.cancel(false);
                                  }
                                  //
                                },
                                exec))
                    .toArray(CompletableFuture[]::new))
            .get();
      }
    }
  }

  @Test
  public void reproVertx() throws Exception {
    var threads = 32;
    try (var exec = Executors.newFixedThreadPool(threads)) {
      for (int i = 0; i < 10000; i++) {
        CompletableFuture.allOf(
                IntStream.range(0, threads)
                    .mapToObj(
                        x ->
                            CompletableFuture.runAsync(
                                () -> {
                                  for (int i1 = 0; i1 < 2; i1++) {
                                    var sem = new Semaphore(0);

                                    var timerId = vertx.setPeriodic(1, 1, tid -> sem.release());

                                    try {
                                      soft.assertThat(
                                              sem.tryAcquire(asyncTimeout.toMillis(), MILLISECONDS))
                                          .isTrue();
                                    } catch (InterruptedException e) {
                                      throw new RuntimeException(e);
                                    }

                                    vertx.cancelTimer(timerId);
                                  }
                                  //
                                },
                                exec))
                    .toArray(CompletableFuture[]::new))
            .get();
      }
    }
  }
}
