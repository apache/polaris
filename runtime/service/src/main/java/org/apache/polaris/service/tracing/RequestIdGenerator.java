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

package org.apache.polaris.service.tracing;

import com.google.common.annotations.VisibleForTesting;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class RequestIdGenerator {
  private volatile String baseDefaultUuid = UUID.randomUUID().toString();
  static final AtomicLong COUNTER = new AtomicLong();
  static final Long COUNTER_SOFT_MAX = Long.MAX_VALUE / 2;

  public String generateRequestId() {
    long currentCounter = COUNTER.incrementAndGet();
    String requestId = baseDefaultUuid + "_" + currentCounter;
    if (currentCounter >= COUNTER_SOFT_MAX) {
      synchronized (this) {
        if (COUNTER.get() >= COUNTER_SOFT_MAX) {
          // If we get anywhere close to danger of overflowing Long (which, theoretically,
          // would be extremely unlikely) rotate the UUID and start again.
          baseDefaultUuid = UUID.randomUUID().toString();
          COUNTER.set(0);
        }
      }
    }
    return requestId;
  }

  @VisibleForTesting
  public void setCounter(long counter) {
    COUNTER.set(counter);
  }
}
