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
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
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
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

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
  public @NonNull BaseResult bootstrapPolarisService(@NonNull PolarisCallContext callCtx) {
    throw illegalMethodError("bootstrapPolarisService");
  }

  @Override
  public @NonNull BaseResult purge(@NonNull PolarisCallContext callCtx) {
    throw illegalMethodError("purge");
  }

  @Override
  public @NonNull EntityResult readEntityByName(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull String name) {
    throw illegalMethodError("readEntityByName");
  }

  @Override
  public @NonNull ListEntitiesResult listEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    throw illegalMethodError("listEntities");
  }

  @Override
  public @NonNull Page<PolarisBaseEntity> listFullEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    throw illegalMethodError("listFullEntities");
  }

  @Override
  public @NonNull GenerateEntityIdResult generateNewEntityId(@NonNull PolarisCallContext callCtx) {
    throw illegalMethodError("generateNewEntityId");
  }

  @Override
  public @NonNull CreatePrincipalResult createPrincipal(
      @NonNull PolarisCallContext callCtx, @NonNull PrincipalEntity principal) {
    throw illegalMethodError("createPrincipal");
  }

  @Override
  public @NonNull PrincipalSecretsResult loadPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
    throw illegalMethodError("loadPrincipalSecrets");
  }

  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    throw illegalMethodError("deletePrincipalSecrets");
  }

  @Override
  public @NonNull PrincipalSecretsResult rotatePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash) {
    throw illegalMethodError("rotatePrincipalSecrets");
  }

  @Override
  public @NonNull PrincipalSecretsResult resetPrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      long principalId,
      @NonNull String resolvedClientId,
      String customClientSecret) {
    throw illegalMethodError("resetPrincipalSecrets");
  }

  @Override
  public @NonNull CreateCatalogResult createCatalog(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity catalog,
      @NonNull List<PolarisEntityCore> principalRoles) {
    throw illegalMethodError("createCatalog");
  }

  @Override
  public @NonNull EntityResult createEntityIfNotExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    throw illegalMethodError("createEntityIfNotExists");
  }

  @Override
  public @NonNull EntitiesResult createEntitiesIfNotExist(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull List<? extends PolarisBaseEntity> entities) {
    throw illegalMethodError("createEntitiesIfNotExist");
  }

  @Override
  public @NonNull EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    pendingUpdates.add(new EntityWithPath(catalogPath, entity));
    return new EntityResult(entity);
  }

  @Override
  public @NonNull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx, @NonNull List<EntityWithPath> entities) {
    throw illegalMethodError("updateEntitiesPropertiesIfNotChanged");
  }

  @Override
  public @NonNull EntityResult renameEntity(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NonNull PolarisEntity renamedEntity) {
    throw illegalMethodError("renameEntity");
  }

  @Override
  public @NonNull DropEntityResult dropEntityIfExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    throw illegalMethodError("dropEntityIfExists");
  }

  @Override
  public @NonNull PrivilegeResult grantUsageOnRoleToGrantee(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee) {
    throw illegalMethodError("grantUsageOnRoleToGrantee");
  }

  @Override
  public @NonNull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee) {
    throw illegalMethodError("revokeUsageOnRoleFromGrantee");
  }

  @Override
  public @NonNull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege) {
    throw illegalMethodError("grantPrivilegeOnSecurableToRole");
  }

  @Override
  public @NonNull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege) {
    throw illegalMethodError("revokePrivilegeOnSecurableFromRole");
  }

  @Override
  public @NonNull LoadGrantsResult loadGrantsOnSecurable(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore securable) {
    throw illegalMethodError("loadGrantsOnSecurable");
  }

  @Override
  public @NonNull LoadGrantsResult loadGrantsToGrantee(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore grantee) {
    throw illegalMethodError("loadGrantsToGrantee");
  }

  @Override
  public @NonNull ChangeTrackingResult loadEntitiesChangeTracking(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEntityId> entityIds) {
    throw illegalMethodError("loadEntitiesChangeTracking");
  }

  @Override
  public @NonNull EntityResult loadEntity(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @NonNull PolarisEntityType entityType) {
    throw illegalMethodError("loadEntity");
  }

  @Override
  public @NonNull EntitiesResult loadTasks(
      @NonNull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    throw illegalMethodError("loadTasks");
  }

  @Override
  public @NonNull ScopedCredentialsResult getSubscopedCredsForEntity(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      @NonNull PolarisEntityType entityType,
      boolean allowListOperation,
      @NonNull Set<String> allowedReadLocations,
      @NonNull Set<String> allowedWriteLocations,
      @NonNull PolarisPrincipal polarisPrincipal,
      Optional<String> refreshCredentialsEndpoint,
      @NonNull CredentialVendingContext credentialVendingContext) {
    return delegate.getSubscopedCredsForEntity(
        callCtx,
        catalogId,
        entityId,
        entityType,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations,
        polarisPrincipal,
        refreshCredentialsEndpoint,
        credentialVendingContext);
  }

  @Override
  public @NonNull ResolvedEntityResult loadResolvedEntityById(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    throw illegalMethodError("loadResolvedEntityById");
  }

  @Override
  public @NonNull ResolvedEntityResult loadResolvedEntityByName(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull String entityName) {
    throw illegalMethodError("loadResolvedEntityByName");
  }

  @Override
  public @NonNull ResolvedEntitiesResult loadResolvedEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityType entityType,
      @NonNull List<PolarisEntityId> entityIds) {
    throw illegalMethodError("loadResolvedEntities");
  }

  @Override
  public @NonNull ResolvedEntityResult refreshResolvedEntity(
      @NonNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NonNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    throw illegalMethodError("refreshResolvedEntity");
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @NonNull PolarisCallContext callContext, T entity) {
    throw illegalMethodError("hasOverlappingSiblings");
  }

  @Override
  public @NonNull PolicyAttachmentResult attachPolicyToEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> targetCatalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy,
      Map<String, String> parameters) {
    throw illegalMethodError("attachPolicyToEntity");
  }

  @Override
  public @NonNull PolicyAttachmentResult detachPolicyFromEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy) {
    throw illegalMethodError("detachPolicyFromEntity");
  }

  @Override
  public @NonNull LoadPolicyMappingsResult loadPoliciesOnEntity(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisEntityCore target) {
    throw illegalMethodError("loadPoliciesOnEntity");
  }

  @Override
  public @NonNull LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore target,
      @NonNull PolicyType policyType) {
    throw illegalMethodError("loadPoliciesOnEntityByType");
  }

  @Override
  public void writeEvents(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEvent> polarisEvents) {
    throw illegalMethodError("writeEvents");
  }
}
