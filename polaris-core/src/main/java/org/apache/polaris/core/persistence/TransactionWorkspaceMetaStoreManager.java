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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.cache.PolarisRemoteCache.CachedEntryResult;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps an existing impl of PolarisMetaStoreManager and delegates expected "read" operations
 * through to the wrapped instance while throwing errors on unexpected operations or enqueuing
 * expected write operations into a collection to be committed as a single atomic unit.
 *
 * <p>Note that as long as the server-side multi-commit transaction semantics are effectively only
 * SERIALIZABLE isolation (i.e. if we can resolve all UpdateRequirements "statically" before the set
 * of commits and translate these into an atomic collection of compare-and-swap operations to apply
 * the transaction), this workspace should also reject readEntity/loadEntity operations to avoid
 * implying that any reads from this transaction workspace include writes performed into this
 * transaction workspace that haven't yet been committed.
 *
 * <p>Not thread-safe; instances should only be used within a single request context and should not
 * be reused between requests.
 */
public class TransactionWorkspaceMetaStoreManager implements PolarisMetaStoreManager {
  private final PolarisMetaStoreManager delegate;

  // TODO: If we want to support the semantic of opening a transaction in which multiple
  // reads and writes occur on the same entities, where the reads are expected to see the writes
  // within the transaction workspace that haven't actually been committed, we can augment this
  // class by allowing these pendingUpdates to represent the latest state of the entity if we
  // also increment entityVersion. We'd need to store both a "latest view" of all updated entities
  // to serve reads within the same transaction while also storing the ordered list of
  // pendingUpdates that ultimately need to be applied in order within the real MetaStoreManager.
  private final List<EntityWithPath> pendingUpdates = new ArrayList<>();

  public TransactionWorkspaceMetaStoreManager(PolarisMetaStoreManager delegate) {
    this.delegate = delegate;
  }

  public List<EntityWithPath> getPendingUpdates() {
    return ImmutableList.copyOf(pendingUpdates);
  }

  @Override
  public BaseResult bootstrapPolarisService(@NotNull PolarisCallContext callCtx) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "bootstrapPolarisService");
    return null;
  }

  @Override
  public BaseResult purge(@NotNull PolarisCallContext callCtx) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "purge");
    return null;
  }

  @Override
  public PolarisMetaStoreManager.EntityResult readEntityByName(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType,
      @NotNull String name) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "readEntityByName");
    return null;
  }

  @Override
  public ListEntitiesResult listEntities(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityType entityType,
      @NotNull PolarisEntitySubType entitySubType) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "listEntities");
    return null;
  }

  @Override
  public GenerateEntityIdResult generateNewEntityId(@NotNull PolarisCallContext callCtx) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "generateNewEntityId");
    return null;
  }

  @Override
  public CreatePrincipalResult createPrincipal(
      @NotNull PolarisCallContext callCtx, @NotNull PolarisBaseEntity principal) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "createPrincipal");
    return null;
  }

  @Override
  public PrincipalSecretsResult loadPrincipalSecrets(
      @NotNull PolarisCallContext callCtx, @NotNull String clientId) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "loadPrincipalSecrets");
    return null;
  }

  @Override
  public PrincipalSecretsResult rotatePrincipalSecrets(
      @NotNull PolarisCallContext callCtx,
      @NotNull String clientId,
      long principalId,
      boolean reset,
      @NotNull String oldSecretHash) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "rotatePrincipalSecrets");
    return null;
  }

  @Override
  public CreateCatalogResult createCatalog(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisBaseEntity catalog,
      @NotNull List<PolarisEntityCore> principalRoles) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "createCatalog");
    return null;
  }

  @Override
  public EntityResult createEntityIfNotExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "createEntityIfNotExists");
    return null;
  }

  @Override
  public EntitiesResult createEntitiesIfNotExist(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull List<? extends PolarisBaseEntity> entities) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "createEntitiesIfNotExist");
    return null;
  }

  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisBaseEntity entity) {
    pendingUpdates.add(new EntityWithPath(catalogPath, entity));
    return new EntityResult(entity);
  }

  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NotNull PolarisCallContext callCtx, @NotNull List<EntityWithPath> entities) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "updateEntitiesPropertiesIfNotChanged");
    return null;
  }

  @Override
  public EntityResult renameEntity(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NotNull PolarisEntity renamedEntity) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "renameEntity");
    return null;
  }

  @Override
  public DropEntityResult dropEntityIfExists(
      @NotNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "dropEntityIfExists");
    return null;
  }

  @Override
  public PrivilegeResult grantUsageOnRoleToGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "grantUsageOnRoleToGrantee");
    return null;
  }

  @Override
  public PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NotNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NotNull PolarisEntityCore role,
      @NotNull PolarisEntityCore grantee) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "revokeUsageOnRoleFromGrantee");
    return null;
  }

  @Override
  public PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "grantPrivilegeOnSecurableToRole");
    return null;
  }

  @Override
  public PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NotNull PolarisCallContext callCtx,
      @NotNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NotNull PolarisEntityCore securable,
      @NotNull PolarisPrivilege privilege) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "revokePrivilegeOnSecurableFromRole");
    return null;
  }

  @Override
  public LoadGrantsResult loadGrantsOnSecurable(
      @NotNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "loadGrantsOnSecurable");
    return null;
  }

  @Override
  public LoadGrantsResult loadGrantsToGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "loadGrantsToGrantee");
    return null;
  }

  @Override
  public ChangeTrackingResult loadEntitiesChangeTracking(
      @NotNull PolarisCallContext callCtx, @NotNull List<PolarisEntityId> entityIds) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "loadEntitiesChangeTracking");
    return null;
  }

  @Override
  public EntityResult loadEntity(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "loadEntity");
    return null;
  }

  @Override
  public EntitiesResult loadTasks(
      @NotNull PolarisCallContext callCtx, String executorId, int limit) {
    callCtx.getDiagServices().fail("illegal_method_in_transaction_workspace", "loadTasks");
    return null;
  }

  @Override
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      boolean allowListOperation,
      @NotNull Set<String> allowedReadLocations,
      @NotNull Set<String> allowedWriteLocations) {
    return delegate.getSubscopedCredsForEntity(
        callCtx,
        catalogId,
        entityId,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations);
  }

  @Override
  public ValidateAccessResult validateAccessToLocations(
      @NotNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @NotNull Set<PolarisStorageActions> actions,
      @NotNull Set<String> locations) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "validateAccessToLocations");
    return null;
  }

  @Override
  public CachedEntryResult loadCachedEntryById(
      @NotNull PolarisCallContext callCtx, long entityCatalogId, long entityId) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "loadCachedEntryById");
    return null;
  }

  @Override
  public CachedEntryResult loadCachedEntryByName(
      @NotNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NotNull PolarisEntityType entityType,
      @NotNull String entityName) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "loadCachedEntryByName");
    return null;
  }

  @Override
  public CachedEntryResult refreshCachedEntity(
      @NotNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NotNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    callCtx
        .getDiagServices()
        .fail("illegal_method_in_transaction_workspace", "refreshCachedEntity");
    return null;
  }
}
