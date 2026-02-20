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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/** Authorization decision returned by authorizer implementations. */
public final class AuthorizationDecision {
  private static final AuthorizationDecision ALLOW = new AuthorizationDecision(true, null);

  private final boolean allowed;
  private final String message;

  private AuthorizationDecision(boolean allowed, @Nullable String message) {
    this.allowed = allowed;
    this.message = message;
  }

  public static AuthorizationDecision allow() {
    return ALLOW;
  }

  public static AuthorizationDecision deny(@Nullable String message) {
    return new AuthorizationDecision(false, message);
  }

  public boolean isAllowed() {
    return allowed;
  }

  @Nullable
  public String getMessage() {
    return message;
  }

  @Nonnull
  public String getMessageOrDefault(@Nonnull String defaultMessage) {
    return message == null ? defaultMessage : message;
  }
}
