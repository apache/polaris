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
package org.apache.polaris.service.auth.internal;

import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.auth.PolarisCredential;
import org.jspecify.annotations.Nullable;

/**
 * Sub-interface of {@link PolarisCredential}s that represent internally-managed principals, i.e.,
 * principals that have a backing entity in the Polaris metastore.
 *
 * <p>When the authenticator receives a credential that implements this interface, it will look up
 * the principal entity in the metastore and require it to exist.
 *
 * <p>Custom token brokers that produce credentials for internally-managed principals should
 * implement this interface so that those credentials are not silently misclassified as external.
 */
@PolarisImmutable
public interface InternalPolarisCredential extends PolarisCredential {

  static InternalPolarisCredential of(
      @Nullable Long principalId, @Nullable String principalName, Set<String> principalRoles) {
    return ImmutableInternalPolarisCredential.builder()
        .principalId(principalId)
        .principalName(principalName)
        .principalRoles(principalRoles)
        .build();
  }

  /** The principal name, or null if unknown. Used for principal lookups by name. */
  @Nullable // switch from non-nullable to nullable
  @Override
  String getPrincipalName();

  /** The principal id, or null if unknown. Used for principal lookups by id. */
  @Nullable Long getPrincipalId();
}
