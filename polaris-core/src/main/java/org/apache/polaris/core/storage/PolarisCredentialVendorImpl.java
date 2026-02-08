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
package org.apache.polaris.core.storage;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;

/**
 * Standalone implementation of {@link PolarisCredentialVendor} that decouples credential vending
 * from the metastore manager.
 *
 * <p>This implementation extracts the storage configuration from the entity's internal properties,
 * resolves the appropriate cloud-specific {@link PolarisStorageIntegration} via the provider, and
 * delegates to it for the actual credential vending (e.g. AWS STS AssumeRole, GCP token exchange,
 * Azure token generation).
 */
public class PolarisCredentialVendorImpl implements PolarisCredentialVendor {

  private final PolarisMetaStoreManager metaStoreManager;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final PolarisDiagnostics diagnostics;

  public PolarisCredentialVendorImpl(
      PolarisMetaStoreManager metaStoreManager,
      PolarisStorageIntegrationProvider storageIntegrationProvider,
      PolarisDiagnostics diagnostics) {
    this.metaStoreManager = metaStoreManager;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.diagnostics = diagnostics;
  }

  @Override
  @Nonnull
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint,
      @Nonnull CredentialVendingContext credentialVendingContext) {

    // Load entity by ID and delegate to the entity-based overload
    EntityResult reloadedEntity =
        metaStoreManager.loadEntity(callCtx, catalogId, entityId, entityType);
    if (reloadedEntity.getReturnStatus() != BaseResult.ReturnStatus.SUCCESS) {
      return new ScopedCredentialsResult(
          reloadedEntity.getReturnStatus(), reloadedEntity.getExtraInformation());
    }

    return getSubscopedCredsForEntity(
        callCtx,
        new PolarisEntity(reloadedEntity.getEntity()),
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations,
        polarisPrincipal,
        refreshCredentialsEndpoint,
        credentialVendingContext);
  }

  @Override
  @Nonnull
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntity entity,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint,
      @Nonnull CredentialVendingContext credentialVendingContext) {

    diagnostics.check(
        !allowedReadLocations.isEmpty() || !allowedWriteLocations.isEmpty(),
        "allowed_locations_to_subscope_is_required");

    // extract storage config directly from the provided entity
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(diagnostics, entity);
    PolarisStorageIntegration<PolarisStorageConfigurationInfo> storageIntegration =
        storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);

    diagnostics.checkNotNull(
        storageIntegration,
        "storage_integration_not_exists",
        "catalogId={}, entityId={}",
        entity.getCatalogId(),
        entity.getId());

    try {
      StorageAccessConfig storageAccessConfig =
          storageIntegration.getSubscopedCreds(
              callCtx.getRealmConfig(),
              allowListOperation,
              allowedReadLocations,
              allowedWriteLocations,
              polarisPrincipal,
              refreshCredentialsEndpoint,
              credentialVendingContext);
      return new ScopedCredentialsResult(storageAccessConfig);
    } catch (Exception ex) {
      return new ScopedCredentialsResult(
          BaseResult.ReturnStatus.SUBSCOPE_CREDS_ERROR, ex.getMessage());
    }
  }
}
