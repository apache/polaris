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
package org.apache.polaris.persistence.nosql.impl.commits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestExclusiveCommitSynchronizer {
  @Test
  public void queuedCommitAcquiresBeforeNewArrival() throws Exception {
    var sync = new ExclusiveCommitSynchronizer();
    var field = ExclusiveCommitSynchronizer.class.getDeclaredField("semaphore");
    field.setAccessible(true);
    var semaphore = (Semaphore) field.get(sync);
    assertThat(semaphore.isFair()).isTrue();
    assertThat(sync.before(0)).isTrue();
    var releaseWaiter = new CountDownLatch(1);
    var waiterEntered = new CountDownLatch(1);
    var executor = Executors.newSingleThreadExecutor();
    try {
      var waiter =
          executor.submit(
              () -> {
                if (!sync.before(TimeUnit.SECONDS.toNanos(5))) {
                  throw new AssertionError("Timed out waiting for permit");
                }
                try {
                  waiterEntered.countDown();
                  assertThat(releaseWaiter.await(5, TimeUnit.SECONDS)).isTrue();
                } finally {
                  sync.after();
                }
                return true;
              });
      var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!semaphore.hasQueuedThreads() && System.nanoTime() < deadline) {
        Thread.sleep(1);
      }
      assertThat(semaphore.hasQueuedThreads()).isTrue();
      sync.after();
      // A timed zero-wait acquisition must not barge ahead of the queued waiter.
      var barged = sync.before(0);
      if (barged) {
        sync.after();
      }
      assertThat(barged).isFalse();
      assertThat(waiterEntered.await(5, TimeUnit.SECONDS)).isTrue();
      releaseWaiter.countDown();
      assertThat(waiter.get(5, TimeUnit.SECONDS)).isTrue();
      assertThat(sync.before(0)).isTrue();
      sync.after();
    } finally {
      releaseWaiter.countDown();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  public void interruptedWaitDoesNotAcquireOrReleasePermit() {
    var sync = new ExclusiveCommitSynchronizer();
    assertThat(sync.before(0)).isTrue();
    try {
      Thread.currentThread().interrupt();
      assertThatThrownBy(() -> sync.before(TimeUnit.SECONDS.toNanos(5)))
          .isInstanceOf(RuntimeException.class)
          .hasCauseInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
    assertThat(sync.before(0)).isFalse();
    sync.after();
    assertThat(sync.before(0)).isTrue();
    sync.after();
  }
}
