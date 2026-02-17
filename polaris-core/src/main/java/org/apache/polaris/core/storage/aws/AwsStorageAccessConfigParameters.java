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
package org.apache.polaris.core.storage.aws;

import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.cache.StorageAccessConfigParameters;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Storage access config parameters for AWS. Includes principal name and credential vending context
 * fields that affect AWS STS AssumeRole behavior (session tags, role session name).
 */
@PolarisImmutable
public interface AwsStorageAccessConfigParameters extends StorageAccessConfigParameters {

  @Value.Parameter(order = 1)
  String realmId();

  @Value.Parameter(order = 2)
  long catalogId();

  @Value.Parameter(order = 3)
  @Nullable
  String storageConfigSerializedStr();

  @Override
  @Value.Parameter(order = 4)
  boolean allowedListAction();

  @Override
  @Value.Parameter(order = 5)
  Set<String> allowedReadLocations();

  @Override
  @Value.Parameter(order = 6)
  Set<String> allowedWriteLocations();

  @Override
  @Value.Parameter(order = 7)
  Optional<String> refreshCredentialsEndpoint();

  @Override
  @Value.Parameter(order = 8)
  Optional<String> principalName();

  /**
   * Whether the principal name should be embedded in the AWS STS role session name. This is {@code
   * true} only when {@code INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL} is enabled. Note that
   * {@link #principalName()} may be present even when this is {@code false} (e.g. when only session
   * tags are enabled, the principal name is needed for the {@code polaris:principal} tag and for
   * cache key differentiation, but should not appear in the role session name).
   */
  @Value.Parameter(order = 9)
  boolean includePrincipalInRoleSessionName();

  /**
   * Credential vending context for session tags. Overrides the default from {@link
   * StorageAccessConfigParameters#credentialVendingContext()}.
   */
  @Value.Parameter(order = 10)
  @Override
  CredentialVendingContext credentialVendingContext();

  static AwsStorageAccessConfigParameters of(
      String realmId,
      PolarisEntity entity,
      boolean allowedListAction,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint,
      Optional<String> principalName,
      boolean includePrincipalInRoleSessionName,
      CredentialVendingContext credentialVendingContext) {
    String storageConfigSerializedStr =
        entity
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getStorageConfigInfoPropertyName());
    return ImmutableAwsStorageAccessConfigParameters.of(
        realmId,
        entity.getCatalogId(),
        storageConfigSerializedStr,
        allowedListAction,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint,
        principalName,
        includePrincipalInRoleSessionName,
        credentialVendingContext);
  }
}
