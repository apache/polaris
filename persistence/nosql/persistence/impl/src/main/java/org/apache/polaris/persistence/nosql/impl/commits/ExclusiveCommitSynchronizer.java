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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

final class ExclusiveCommitSynchronizer implements CommitSynchronizer {
  private record SyncKey(String realmId, String refName) {}

  private static final Map<SyncKey, CommitSynchronizer> LOCAL_COMMIT_SYNC =
      new ConcurrentHashMap<>();

  private final Semaphore semaphore = new Semaphore(1);

  static CommitSynchronizer forKey(String realmId, String refName) {
    return LOCAL_COMMIT_SYNC.computeIfAbsent(
        new SyncKey(realmId, refName), k -> new ExclusiveCommitSynchronizer());
  }

  @Override
  public void after() {
    semaphore.release();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored") // fine in this case, it'll time out
  @Override
  public void before(long nanosRemaining) {
    try {
      semaphore.tryAcquire(nanosRemaining, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
