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

import static java.lang.Long.highestOneBit;

import java.util.concurrent.atomic.AtomicInteger;

final class SleepingFairRetries implements FairRetries {

  private final AtomicInteger[] prioTasks = new AtomicInteger[16];

  static final SleepingFairRetries INSTANCE = new SleepingFairRetries();

  SleepingFairRetries() {
    for (int i = 0; i < prioTasks.length; i++) {
      prioTasks[i] = new AtomicInteger();
    }
  }

  @Override
  public int beforeAttempt(int retries, int prevPrio) {
    var prio = Math.min(prioTasks.length - 1, retries);

    if (prio != prevPrio) {
      if (prevPrio >= 0) {
        prioTasks[prevPrio].decrementAndGet();
      }

      prioTasks[prio].incrementAndGet();
    }

    var numHigher = 0L;
    for (int i = prioTasks.length - 1; i >= prio; i--) {
      numHigher += prioTasks[i].get();
    }
    var n = highestOneBit(numHigher);
    try {
      Thread.sleep(n);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return prio;
  }

  @Override
  public void done(int prio) {
    prioTasks[prio].decrementAndGet();
  }
}
