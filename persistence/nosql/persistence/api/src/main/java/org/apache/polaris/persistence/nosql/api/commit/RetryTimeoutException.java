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
package org.apache.polaris.persistence.nosql.api.commit;

import java.time.Duration;

/**
 * Thrown to indicate that a retryable ({@linkplain Committer#commit(CommitRetryable) commit})
 * attempt eventually failed due to a timeout.
 */
public final class RetryTimeoutException extends Exception {

  private final int retry;
  private final long timeNanos;

  public RetryTimeoutException(int retry, long timeNanos) {
    super("Retry timeout after " + Duration.ofNanos(timeNanos) + ", " + retry + " retries");
    this.retry = retry;
    this.timeNanos = timeNanos;
  }

  public int getRetry() {
    return retry;
  }

  public long getTimeNanos() {
    return timeNanos;
  }
}
