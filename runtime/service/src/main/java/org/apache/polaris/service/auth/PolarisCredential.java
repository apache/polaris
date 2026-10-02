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
import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

/**
 * A Quarkus Security {@link Credential} exposing Polaris-specific attributes.
 *
 * <p>Credentials where {@link #isExternal()} returns {@code true} represent externally-managed
 * principals: the authenticator takes the principal name and roles as-is without performing a
 * metastore lookup. All other credentials — including plain {@link PolarisCredential} instances
 * returned by a custom token broker — are treated as internal and require a backing principal
 * entity in the Polaris metastore.
 */
@PolarisImmutable
public interface PolarisCredential extends Credential {

  /**
   * Creates a new {@link PolarisCredential} with the given principal id, name and roles. The
   * returned credential is considered internal, and therefore requires a backing principal entity
   * in the Polaris metastore.
   */
  static PolarisCredential of(
      @Nullable Long principalId, @Nullable String principalName, Set<String> principalRoles) {
    return ImmutablePolarisCredential.builder()
        .principalId(principalId)
        .principalName(principalName)
        .principalRoles(principalRoles)
        .build();
  }

  /**
   * Creates a new {@link PolarisCredential} with the given principal name and roles. The returned
   * credential is considered external, and therefore does not require a backing principal entity in
   * the Polaris metastore.
   */
  static PolarisCredential ofExternal(@Nullable String principalName, Set<String> principalRoles) {
    return ImmutablePolarisCredential.builder()
        .principalName(principalName)
        .principalRoles(principalRoles)
        .external(true)
        .build();
  }

  /**
   * Whether this credential represents an externally-managed principal not backed by the Polaris
   * metastore. Defaults to {@code false}.
   */
  @Value.Default
  default boolean isExternal() {
    return false;
  }

  /**
   * The principal id, or null if the credential does not carry one.
   *
   * <p>Principal IDs are used solely for principal entity lookups by id in the metastore. Such
   * lookups are only relevant for internal principals; therefore, the value returned by this method
   * is always ignored when {@link #isExternal()} is true.
   */
  @Nullable Long getPrincipalId();

  /**
   * The principal name, or null if the credential does not carry one. A name is not guaranteed to
   * be present here; it is the authenticator's responsibility to validate it and reject credentials
   * that lack a required name.
   */
  @Nullable String getPrincipalName();

  /** The principal roles, or empty if the principal has no roles. */
  Set<String> getPrincipalRoles();
}
