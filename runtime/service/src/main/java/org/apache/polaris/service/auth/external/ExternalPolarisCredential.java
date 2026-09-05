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
package org.apache.polaris.service.auth.external;

import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.auth.PolarisCredential;
import org.jspecify.annotations.Nullable;

/**
 * Sub-interface of {@link PolarisCredential}s that represents externally-managed principals, i.e.,
 * principals that are <em>not</em> backed by an entity in the Polaris metastore.
 *
 * <p>When the authenticator receives a credential that implements this interface, it does not look
 * up any principal entity in the metastore; the principal name and roles are taken as-is from the
 * credential.
 *
 * <p>External credentials are only produced when an external IDP is configured and the principal
 * mode is {@link org.apache.polaris.service.auth.PrincipalMode#EXTERNAL}. Any other credential —
 * including a plain {@link PolarisCredential} returned by a custom token broker — is treated as
 * internal and requires a backing principal entity.
 */
@PolarisImmutable
public interface ExternalPolarisCredential extends PolarisCredential {

  static ExternalPolarisCredential of(@Nullable String principalName, Set<String> principalRoles) {
    return ImmutableExternalPolarisCredential.builder()
        .principalName(principalName)
        .principalRoles(principalRoles)
        .build();
  }
}
