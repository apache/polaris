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
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;

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
  private final PolarisDiagnostics diagnostics;
  private final PolarisMetaStoreManager delegate;

  // TODO: If we want to support the semantic of opening a transaction in which multiple
  // reads and writes occur on the same entities, where the reads are expected to see the writes
  // within the transaction workspace that haven't actually been committed, we can augment this
  // class by allowing these pendingUpdates to represent the latest state of the entity if we
  // also increment entityVersion. We'd need to store both a "latest view" of all updated entities
  // to serve reads within the same transaction while also storing the ordered list of
  // pendingUpdates that ultimately need to be applied in order within the real MetaStoreManager.
  private final List<EntityWithPath> pendingUpdates = new ArrayList<>();

  public TransactionWorkspaceMetaStoreManager(
      PolarisDiagnostics diagnostics, PolarisMetaStoreManager delegate) {
    this.diagnostics = diagnostics;
    this.delegate = delegate;
  }

  private RuntimeException illegalMethodError(String methodName) {
    return diagnostics.fail("illegal_method_in_transaction_workspace", methodName);
  }

  public List<EntityWithPath> getPendingUpdates() {
    return ImmutableList.copyOf(pendingUpdates);
  }

  @Override
  public @Nonnull BaseResult bootstrapPolarisService(@Nonnull PolarisCallContext callCtx) {
    throw illegalMethodError("bootstrapPolarisService");
  }

  @Override
  public @Nonnull BaseResult purge(@Nonnull PolarisCallContext callCtx) {
    throw illegalMethodError("purge");
  }

  @Override
  public @Nonnull EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    throw illegalMethodError("readEntityByName");
  }

  @Override
  public @Nonnull ListEntitiesResult listEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    throw illegalMethodError("listEntities");
  }

  @Override
  public @Nonnull Page<PolarisBaseEntity> listFullEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    throw illegalMethodError("listFullEntities");
  }

  @Override
  public @Nonnull GenerateEntityIdResult generateNewEntityId(@Nonnull PolarisCallContext callCtx) {
    throw illegalMethodError("generateNewEntityId");
  }

  @Override
  public @Nonnull CreatePrincipalResult createPrincipal(
      @Nonnull PolarisCallContext callCtx, @Nonnull PrincipalEntity principal) {
    throw illegalMethodError("createPrincipal");
  }

  @Override
  public @Nonnull PrincipalSecretsResult loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    throw illegalMethodError("loadPrincipalSecrets");
  }

  @Override
  public void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    throw illegalMethodError("deletePrincipalSecrets");
  }

  @Override
  public @Nonnull PrincipalSecretsResult rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    throw illegalMethodError("rotatePrincipalSecrets");
  }

  @Override
  public @Nonnull PrincipalSecretsResult resetPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      long principalId,
      @Nonnull String resolvedClientId,
      String customClientSecret) {
    throw illegalMethodError("resetPrincipalSecrets");
  }

  @Override
  public @Nonnull CreateCatalogResult createCatalog(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity catalog,
      @Nonnull List<PolarisEntityCore> principalRoles) {
    throw illegalMethodError("createCatalog");
  }

  @Override
  public @Nonnull EntityResult createEntityIfNotExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    throw illegalMethodError("createEntityIfNotExists");
  }

  @Override
  public @Nonnull EntitiesResult createEntitiesIfNotExist(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull List<? extends PolarisBaseEntity> entities) {
    throw illegalMethodError("createEntitiesIfNotExist");
  }

  @Override
  public @Nonnull EntityResult updateEntityPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entity) {
    pendingUpdates.add(new EntityWithPath(catalogPath, entity));
    return new EntityResult(entity);
  }

  @Override
  public @Nonnull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<EntityWithPath> entities) {
    throw illegalMethodError("updateEntitiesPropertiesIfNotChanged");
  }

  @Override
  public @Nonnull EntityResult renameEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @Nonnull PolarisEntity renamedEntity) {
    throw illegalMethodError("renameEntity");
  }

  @Override
  public @Nonnull DropEntityResult dropEntityIfExists(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    throw illegalMethodError("dropEntityIfExists");
  }

  @Override
  public @Nonnull PrivilegeResult grantUsageOnRoleToGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    throw illegalMethodError("grantUsageOnRoleToGrantee");
  }

  @Override
  public @Nonnull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @Nonnull PolarisEntityCore role,
      @Nonnull PolarisEntityCore grantee) {
    throw illegalMethodError("revokeUsageOnRoleFromGrantee");
  }

  @Override
  public @Nonnull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    throw illegalMethodError("grantPrivilegeOnSecurableToRole");
  }

  @Override
  public @Nonnull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore securable,
      @Nonnull PolarisPrivilege privilege) {
    throw illegalMethodError("revokePrivilegeOnSecurableFromRole");
  }

  @Override
  public @Nonnull LoadGrantsResult loadGrantsOnSecurable(
      @Nonnull PolarisCallContext callCtx, PolarisEntityCore securable) {
    throw illegalMethodError("loadGrantsOnSecurable");
  }

  @Override
  public @Nonnull LoadGrantsResult loadGrantsToGrantee(
      @Nonnull PolarisCallContext callCtx, PolarisEntityCore grantee) {
    throw illegalMethodError("loadGrantsToGrantee");
  }

  @Override
  public @Nonnull ChangeTrackingResult loadEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEntityId> entityIds) {
    throw illegalMethodError("loadEntitiesChangeTracking");
  }

  @Override
  public @Nonnull EntityResult loadEntity(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType) {
    throw illegalMethodError("loadEntity");
  }

  @Override
  public @Nonnull EntitiesResult loadTasks(
      @Nonnull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    throw illegalMethodError("loadTasks");
  }

  @Override
  public @Nonnull ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @Nonnull PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint) {
    return delegate.getSubscopedCredsForEntity(
        callCtx,
        catalogId,
        entityId,
        entityType,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint);
  }

  @Override
  public @Nonnull ResolvedEntityResult loadResolvedEntityById(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    throw illegalMethodError("loadResolvedEntityById");
  }

  @Override
  public @Nonnull ResolvedEntityResult loadResolvedEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull String entityName) {
    throw illegalMethodError("loadResolvedEntityByName");
  }

  @Override
  public @Nonnull ResolvedEntitiesResult loadResolvedEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityType entityType,
      @Nonnull List<PolarisEntityId> entityIds) {
    throw illegalMethodError("loadResolvedEntities");
  }

  @Override
  public @Nonnull ResolvedEntityResult refreshResolvedEntity(
      @Nonnull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @Nonnull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    throw illegalMethodError("refreshResolvedEntity");
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @Nonnull PolarisCallContext callContext, T entity) {
    throw illegalMethodError("hasOverlappingSiblings");
  }

  @Override
  public @Nonnull PolicyAttachmentResult attachPolicyToEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> targetCatalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy,
      Map<String, String> parameters) {
    throw illegalMethodError("attachPolicyToEntity");
  }

  @Override
  public @Nonnull PolicyAttachmentResult detachPolicyFromEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityCore target,
      @Nonnull List<PolarisEntityCore> policyCatalogPath,
      @Nonnull PolicyEntity policy) {
    throw illegalMethodError("detachPolicyFromEntity");
  }

  @Override
  public @Nonnull LoadPolicyMappingsResult loadPoliciesOnEntity(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore target) {
    throw illegalMethodError("loadPoliciesOnEntity");
  }

  @Override
  public @Nonnull LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore target,
      @Nonnull PolicyType policyType) {
    throw illegalMethodError("loadPoliciesOnEntityByType");
  }

  @Override
  public void writeEvents(
      @Nonnull PolarisCallContext callCtx, @Nonnull List<PolarisEvent> polarisEvents) {
    throw illegalMethodError("writeEvents");
  }
}
