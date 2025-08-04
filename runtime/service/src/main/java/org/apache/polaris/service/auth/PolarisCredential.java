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

import io.quarkus.security.credential.Credential;
import jakarta.annotation.Nullable;
import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * A Quarkus Security {@link Credential} exposing Polaris-specific attributes: the principal id,
 * name, and roles.
 */
@PolarisImmutable
public interface PolarisCredential extends Credential {

  static PolarisCredential of(
      @Nullable Long getPrincipalId,
      @Nullable String getPrincipalName,
      Set<String> getPrincipalRoles) {
    return ImmutablePolarisCredential.builder()
        .principalId(getPrincipalId)
        .principalName(getPrincipalName)
        .principalRoles(getPrincipalRoles)
        .build();
  }

  /** The principal id, or null if unknown. Used for principal lookups by id. */
  @Nullable
  Long getPrincipalId();

  /** The principal name, or null if unknown. Used for principal lookups by name. */
  @Nullable
  String getPrincipalName();

  /**
   * The principal roles present in the token. The special {@link
   * DefaultAuthenticator#PRINCIPAL_ROLE_ALL} can be used to denote a request for all principal
   * roles that the principal has access to.
   */
  Set<String> getPrincipalRoles();
}
