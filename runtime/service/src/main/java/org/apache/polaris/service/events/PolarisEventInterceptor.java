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

package org.apache.polaris.service.events;

import java.util.Objects;

/** Synchronous interceptor that can allow, deny, or modify an in-flight Polaris event. */
public interface PolarisEventInterceptor {

  Result intercept(PolarisEvent event);

  record Result(Action action, String reason, PolarisEvent modifiedEvent) {

    public enum Action {
      ALLOW,
      DENY,
      MODIFY
    }

    public Result {
      Objects.requireNonNull(action, "action");
      if (action == Action.DENY) {
        Objects.requireNonNull(reason, "reason");
      }
      if (action == Action.MODIFY) {
        Objects.requireNonNull(modifiedEvent, "modifiedEvent");
      }
    }

    public static Result allow() {
      return new Result(Action.ALLOW, null, null);
    }

    public static Result deny(String reason) {
      return new Result(Action.DENY, reason, null);
    }

    public static Result modify(PolarisEvent modifiedEvent) {
      return new Result(Action.MODIFY, null, modifiedEvent);
    }
  }
}
