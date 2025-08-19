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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;

/**
 * Interface for the necessary "peripheral integration" objects that are logically attached to core
 * persistence entities but which typically involve additional separate external integrations
 * related to identity/auth, kms/secrets storage, etc.
 *
 * <p>Each method in this interface must be atomic, meaning that write operations must either fully
 * succeed with all changes applied, or fail entirely without partial updates. Read operations must
 * provide a consistent view of the data as it existed at the start of the operation.
 *
 * <p>Implementations should orchestrate any necessary multi-phase protocols such as leasing an
 * external resource before committing a reference to the external resource in the Polaris
 * persistence layer, etc.
 */
public interface IntegrationPersistence {
  /**
   * Allows to retrieve to the secrets of a principal given its unique client id
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @return the secrets
   */
  @Nullable
  PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId);

  /**
   * generate and store a client id and associated secrets for a newly created principal entity
   *
   * @param callCtx call context
   * @param principalName name of the principal
   * @param principalId principal id
   */
  @Nonnull
  PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId);

  /**
   * Rotate the secrets of a principal entity, i.e. make the specified main secrets the secondary
   * and generate a new main secret
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @param principalId principal id
   * @param reset true if the principal secrets should be disabled and replaced with a one-time
   *     password
   * @param oldSecretHash the principal secret's old main secret hash
   */
  @Nullable
  PolarisPrincipalSecrets rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash);

  /**
   * Reset the secrets of a principal entity, i.e. make the specified secrets as main and secondary
   * and assign a new client id
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @param principalId principal id
   * @param customClientId the principal secret's old main secret hash
   * @param customClientSecret the principal secret's old main secret hash
   */
  @Nullable
  PolarisPrincipalSecrets resetPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      String customClientId,
      String customClientSecret);

  /**
   * When dropping a principal, we also need to drop the secrets of that principal
   *
   * @param callCtx the call context
   * @param clientId principal client id
   * @param principalId the id of the principal whose secrets are dropped
   */
  void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId);

  /**
   * Create an in-memory storage integration
   *
   * @param callCtx the polaris calllctx
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param polarisStorageConfigurationInfo the storage configuration information
   * @return a storage integration object
   */
  @Nullable
  <T extends PolarisStorageConfigurationInfo> PolarisStorageIntegration<T> createStorageIntegration(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisStorageConfigurationInfo polarisStorageConfigurationInfo);

  /**
   * Persist a storage integration in the metastore
   *
   * @param callContext the polaris call context
   * @param entity the entity of the object
   * @param storageIntegration the storage integration to persist
   */
  <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @Nonnull PolarisCallContext callContext,
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration);

  /**
   * Load the polaris storage integration for a polaris entity (Catalog,Namespace,Table,View)
   *
   * @param callContext the polaris call context
   * @param entity the polaris entity
   * @return a polaris storage integration
   */
  @Nullable
  <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @Nonnull PolarisCallContext callContext, @Nonnull PolarisBaseEntity entity);
}
