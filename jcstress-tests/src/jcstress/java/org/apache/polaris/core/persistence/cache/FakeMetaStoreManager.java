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
package org.apache.polaris.core.persistence.cache;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.*;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;

/**
 * Build a fake meta store manager that returns a single catalog entity. Every time the catalog is
 * returned, an incremented entity version id is used. this is in order to disambiguate the entity
 * returned by the cache and verify whether the cache is thread safe.
 *
 * <p>Think of it as a poor-man's Mockito. It is not a real mock, but it is good enough for this
 * test. More importantly, it is a lot faster to instantiate than Mockito. It allows the stress test
 * to run up to 100x more iterations in the same amount of time.
 *
 * <p>The only implemented methods are `loadResolvedEntityById` and `loadResolvedEntityByName`. Any
 * time they are invoked, regardless of their parameters, the same catalog is returned but with an
 * increased entity version id.
 */
public class FakeMetaStoreManager implements PolarisMetaStoreManager {
  public static final int CATALOG_ID = 42;
  private final Supplier<ResolvedEntityResult> catalogSupplier;

  public FakeMetaStoreManager() {
    final AtomicInteger versionCounter = new AtomicInteger(1);
    this.catalogSupplier =
        () -> {
          int version = versionCounter.getAndIncrement();
          CatalogEntity catalog =
              new CatalogEntity.Builder()
                  .setId(CATALOG_ID)
                  .setInternalProperties(Map.of())
                  .setProperties(Map.of())
                  .setName("test")
                  .setParentId(PolarisEntityConstants.getRootEntityId())
                  .setEntityVersion(version)
                  .build();
          return new ResolvedEntityResult(catalog, version, emptyList());
        };
  }

  /**
   * Utility method that ensures that catalogs creation is thread safe.
   *
   * @return a catalog entity with an incremented version id
   */
  private synchronized ResolvedEntityResult nextResult() {
    return catalogSupplier.get();
  }

  @Override
  public BaseResult bootstrapPolarisService(PolarisCallContext callCtx) {
    return null;
  }

  @Override
  public BaseResult purge(PolarisCallContext callCtx) {
    return null;
  }

  @Override
  public EntityResult readEntityByName(
      PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityType entityType,
      PolarisEntitySubType entitySubType,
      String name) {
    return null;
  }

  @Override
  public ListEntitiesResult listEntities(
      PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityType entityType,
      PolarisEntitySubType entitySubType) {
    return null;
  }

  @Override
  public GenerateEntityIdResult generateNewEntityId(PolarisCallContext callCtx) {
    return null;
  }

  @Override
  public CreatePrincipalResult createPrincipal(
      PolarisCallContext callCtx, PolarisBaseEntity principal) {
    return null;
  }

  @Override
  public CreateCatalogResult createCatalog(
      PolarisCallContext callCtx,
      PolarisBaseEntity catalog,
      List<PolarisEntityCore> principalRoles) {
    return null;
  }

  @Override
  public EntityResult createEntityIfNotExists(
      PolarisCallContext callCtx, List<PolarisEntityCore> catalogPath, PolarisBaseEntity entity) {
    return null;
  }

  @Override
  public EntitiesResult createEntitiesIfNotExist(
      PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      List<? extends PolarisBaseEntity> entities) {
    return null;
  }

  @Override
  public EntityResult updateEntityPropertiesIfNotChanged(
      PolarisCallContext callCtx, List<PolarisEntityCore> catalogPath, PolarisBaseEntity entity) {
    return null;
  }

  @Override
  public EntitiesResult updateEntitiesPropertiesIfNotChanged(
      PolarisCallContext callCtx, List<EntityWithPath> entities) {
    return null;
  }

  @Override
  public EntityResult renameEntity(
      PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      PolarisBaseEntity entityToRename,
      List<PolarisEntityCore> newCatalogPath,
      PolarisEntity renamedEntity) {
    return null;
  }

  @Override
  public DropEntityResult dropEntityIfExists(
      PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      PolarisBaseEntity entityToDrop,
      Map<String, String> cleanupProperties,
      boolean cleanup) {
    return null;
  }

  @Override
  public EntityResult loadEntity(
      PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    return null;
  }

  @Override
  public EntitiesResult loadTasks(PolarisCallContext callCtx, String executorId, int limit) {
    return null;
  }

  @Override
  public ChangeTrackingResult loadEntitiesChangeTracking(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return null;
  }

  @Override
  public ResolvedEntityResult loadResolvedEntityById(
      PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    return nextResult();
  }

  @Override
  public ResolvedEntityResult loadResolvedEntityByName(
      PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      PolarisEntityType entityType,
      String entityName) {
    return nextResult();
  }

  @Override
  public ResolvedEntityResult refreshResolvedEntity(
      PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    return nextResult();
  }

  @Override
  public PrivilegeResult grantUsageOnRoleToGrantee(
      PolarisCallContext callCtx,
      PolarisEntityCore catalog,
      PolarisEntityCore role,
      PolarisEntityCore grantee) {
    return null;
  }

  @Override
  public PrivilegeResult revokeUsageOnRoleFromGrantee(
      PolarisCallContext callCtx,
      PolarisEntityCore catalog,
      PolarisEntityCore role,
      PolarisEntityCore grantee) {
    return null;
  }

  @Override
  public PrivilegeResult grantPrivilegeOnSecurableToRole(
      PolarisCallContext callCtx,
      PolarisEntityCore grantee,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityCore securable,
      PolarisPrivilege privilege) {
    return null;
  }

  @Override
  public PrivilegeResult revokePrivilegeOnSecurableFromRole(
      PolarisCallContext callCtx,
      PolarisEntityCore grantee,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityCore securable,
      PolarisPrivilege privilege) {
    return null;
  }

  @Override
  public LoadGrantsResult loadGrantsOnSecurable(
      PolarisCallContext callCtx, PolarisEntityCore securable) {
    return null;
  }

  @Override
  public LoadGrantsResult loadGrantsToGrantee(
      PolarisCallContext callCtx, PolarisEntityCore grantee) {
    return null;
  }

  @Override
  public PrincipalSecretsResult loadPrincipalSecrets(PolarisCallContext callCtx, String clientId) {
    return null;
  }

  @Override
  public PrincipalSecretsResult rotatePrincipalSecrets(
      PolarisCallContext callCtx,
      String clientId,
      long principalId,
      boolean reset,
      String oldSecretHash) {
    return null;
  }

  @Override
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      boolean allowListOperation,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations) {
    return null;
  }

  @Override
  public PolicyAttachmentResult attachPolicyToEntity(
      PolarisCallContext callCtx,
      List<PolarisEntityCore> targetCatalogPath,
      PolarisEntityCore target,
      List<PolarisEntityCore> policyCatalogPath,
      PolicyEntity policy,
      Map<String, String> parameters) {
    return null;
  }

  @Override
  public PolicyAttachmentResult detachPolicyFromEntity(
      PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      PolarisEntityCore target,
      List<PolarisEntityCore> policyCatalogPath,
      PolicyEntity policy) {
    return null;
  }

  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntity(
      PolarisCallContext callCtx, PolarisEntityCore target) {
    return null;
  }

  @Override
  public LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      PolarisCallContext callCtx, PolarisEntityCore target, PolicyType policyType) {
    return null;
  }
}
