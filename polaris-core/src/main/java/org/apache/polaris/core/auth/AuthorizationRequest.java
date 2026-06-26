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
package org.apache.polaris.core.auth;

import com.google.common.base.Preconditions;
import java.util.List;
import org.jspecify.annotations.NonNull;

/** Full authorization request containing the subject and one or more authorization intents. */
public record AuthorizationRequest(
    @NonNull PolarisPrincipal principal, @NonNull List<AuthorizationIntent> intents) {
  public AuthorizationRequest {
    Preconditions.checkNotNull(principal, "principal must be non-null");
    Preconditions.checkNotNull(intents, "intents must be non-null");
    intents = List.copyOf(intents);
    Preconditions.checkArgument(
        !intents.isEmpty(), "Authorization request must contain at least one intent");
  }
}
