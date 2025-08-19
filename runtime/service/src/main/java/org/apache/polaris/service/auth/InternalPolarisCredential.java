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
package org.apache.polaris.service.auth;

import com.google.common.base.Splitter;
import jakarta.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * A specialized {@link PolarisCredential} used for internal authentication, when Polaris is the
 * identity provider.
 *
 * <p>Such credentials are created by the Polaris service itself, from a JWT token previously issued
 * by Polaris itself.
 *
 * @see JWTBroker
 */
@PolarisImmutable
abstract class InternalPolarisCredential implements PolarisCredential {

  private static final Splitter SCOPE_SPLITTER = Splitter.on(' ').omitEmptyStrings().trimResults();

  static InternalPolarisCredential of(
      String principalName, Long principalId, String clientId, String scope) {
    return ImmutableInternalPolarisCredential.builder()
        .principalName(principalName)
        .principalId(principalId)
        .clientId(clientId)
        .scope(scope)
        .build();
  }

  @Nonnull // switch from nullable to non-nullable
  @Override
  @SuppressWarnings("NullableProblems")
  public abstract String getPrincipalName();

  @Nonnull // switch from nullable to non-nullable
  @Override
  @SuppressWarnings("NullableProblems")
  public abstract Long getPrincipalId();

  @Value.Lazy
  @Override
  public Set<String> getPrincipalRoles() {
    // Polaris stores roles in the scope claim
    return SCOPE_SPLITTER.splitToStream(getScope()).collect(Collectors.toSet());
  }

  abstract String getClientId();

  abstract String getScope();
}
