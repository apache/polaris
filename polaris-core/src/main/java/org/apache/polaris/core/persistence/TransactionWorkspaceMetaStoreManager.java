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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.storage.PolarisStorageActions;

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
  private final PolarisDiagnostics diagnostics;

  public TransactionWorkspaceMetaStoreManager(
      PolarisMetaStoreManager delegate, PolarisDiagnostics diagnostics) {
    this.delegate = delegate;
    this.diagnostics = diagnostics;
  }

  public List<EntityWithPath> getPendingUpdates() {
    return ImmutableList.copyOf(pendingUpdates);
  }

  @Override
  public @Nonnull BaseResult bootstrapPolarisService(@Nonnull PolarisMetaStoreSession session) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "bootstrapPolarisService");
    return null;
  }

  @Override
  public @Nonnull BaseResult purge(@Nonnull PolarisMetaStoreSession session) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "purge");
    return null;
  }

  @Override
  public @Nonnull PolarisMetaStoreManager.EntityResult readEntityByName(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "readEntityByName");
    return null;
  }

  @Override
  public @Nonnull ListEntitiesResult listEntities(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "listEntities");
    return null;
  }

  @Override
  public @Nonnull GenerateEntityIdResult generateNewEntityId(@Nonnull PolarisMetaStoreSession ms) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "generateNewEntityId");
    return null;
  }

  @Override
  public @Nonnull CreatePrincipalResult createPrincipal(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull PolarisBaseEntity principal) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "createPrincipal");
    return null;
  }

  @Override
  public @Nonnull PrincipalSecretsResult loadPrincipalSecrets(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull String clientId) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "loadPrincipalSecrets");
    return null;
  }

  @Override
  public @Nonnull PrincipalSecretsResult rotatePrincipalSecrets(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "rotatePrincipalSecrets");
    return null;
  }

  @Override
  public @Nonnull CreateCatalogResult createCatalog(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "createCatalog");
    return null;
  }

  @Override
  public @Nonnull EntityResult createEntityIfNotExists(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "createEntityIfNotExists");
    return null;
  }

  @Override
  public @Nonnull EntitiesResult createEntitiesIfNotExist(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "createEntitiesIfNotExist");
    return null;
  }

  @Override
  public @Nonnull EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    pendingUpdates.add(new EntityWithPath(catalogPath, entity));
    return new EntityResult(entity);
  }

  @Override
  public @Nonnull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull List<EntityWithPath> entities) {
    diagnostics.fail(
        "illegal_method_in_transaction_workspace", "updateEntitiesPropertiesIfNotChanged");
    return null;
  }

  @Override
  public @Nonnull EntityResult renameEntity(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "renameEntity");
    return null;
  }

  @Override
  public @Nonnull DropEntityResult dropEntityIfExists(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "dropEntityIfExists");
    return null;
  }

  @Override
  public @Nonnull PrivilegeResult grantUsageOnRoleToGrantee(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "grantUsageOnRoleToGrantee");
    return null;
  }

  @Override
  public @Nonnull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "revokeUsageOnRoleFromGrantee");
    return null;
  }

  @Override
  public @Nonnull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "grantPrivilegeOnSecurableToRole");
    return null;
  }

  @Override
  public @Nonnull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisMetaStoreSession ms,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    diagnostics.fail(
        "illegal_method_in_transaction_workspace", "revokePrivilegeOnSecurableFromRole");
    return null;
  }

  @Override
  public @Nonnull LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisMetaStoreSession ms, long securableCatalogId, long securableId) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "loadGrantsOnSecurable");
    return null;
  }

  @Override
  public @Nonnull LoadGrantsResult loadGrantsToGrantee(
      @Nonnull PolarisMetaStoreSession ms, long granteeCatalogId, long granteeId) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "loadGrantsToGrantee");
    return null;
  }

  @Override
  public @Nonnull ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisMetaStoreSession ms, @Nonnull List<PolarisEntityId> entityIds) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "loadEntitiesChangeTracking");
    return null;
  }

  @Override
  @Nonnull
  public EntityResult loadEntity(
      @Nonnull PolarisMetaStoreSession ms, long entityCatalogId, long entityId) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "loadEntity");
    return null;
  }

  @Override
  @Nonnull
  public EntitiesResult loadTasks(
      @Nonnull PolarisMetaStoreSession ms, String executorId, int limit) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "loadTasks");
    return null;
  }

  @Override
  public @Nonnull ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisMetaStoreSession metaStoreSession,
      long catalogId,
      long entityId,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations) {
    return delegate.getSubscopedCredsForEntity(
        metaStoreSession,
        catalogId,
        entityId,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations);
  }

  @Override
  public @Nonnull ValidateAccessResult validateAccessToLocations(
      @Nonnull PolarisMetaStoreSession metaStoreSession,
      long catalogId,
      long entityId,
      @Nonnull Set<PolarisStorageActions> actions,
      @Nonnull Set<String> locations) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "validateAccessToLocations");
    return null;
  }

  @Override
  public @Nonnull CachedEntryResult loadCachedEntryById(
      @Nonnull PolarisMetaStoreSession ms, long entityCatalogId, long entityId) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "loadCachedEntryById");
    return null;
  }

  @Override
  public @Nonnull CachedEntryResult loadCachedEntryByName(
      @Nonnull PolarisMetaStoreSession ms,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "loadCachedEntryByName");
    return null;
  }

  @Override
  public @Nonnull CachedEntryResult refreshCachedEntity(
      @Nonnull PolarisMetaStoreSession ms,
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    diagnostics.fail("illegal_method_in_transaction_workspace", "refreshCachedEntity");
    return null;
  }
}
