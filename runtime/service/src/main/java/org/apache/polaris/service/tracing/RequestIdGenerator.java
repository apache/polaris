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
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class RequestIdGenerator {
  static final Long COUNTER_SOFT_MAX = Long.MAX_VALUE / 2;

  record State(String uuid, long counter) {

    State() {
      this(UUID.randomUUID().toString(), 1);
    }

    String requestId() {
      return String.format("%s_%019d", uuid, counter);
    }

    State increment() {
      return counter >= COUNTER_SOFT_MAX ? new State() : new State(uuid, counter + 1);
    }
  }

  final AtomicReference<State> state = new AtomicReference<>(new State());

  public String generateRequestId() {
    return state.getAndUpdate(State::increment).requestId();
  }

  @VisibleForTesting
  public void setCounter(long counter) {
    state.set(new State(state.get().uuid, counter));
  }
}
