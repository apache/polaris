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

package org.apache.polaris.persistence.nosql.metastore;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class NonFunctionalBasePersistence implements BasePersistence, IntegrationPersistence {
  private static final Logger LOGGER = LoggerFactory.getLogger(NonFunctionalBasePersistence.class);

  @Override
  public long generateNewId(@Nonnull PolarisCallContext callCtx) {
    throw unimplemented();
  }

  @Override
  public void writeEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      // nameOrParentChanged is `true` if originalEntity==null or the parentId or the name changed
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    throw useMetaStoreManager("create/update/rename/delete");
  }

  @Override
  public void writeEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    throw useMetaStoreManager("create/update/rename/delete");
  }

  @Override
  public void writeToGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    throw unimplemented();
  }

  @Override
  public void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    throw unimplemented();
  }

  @Override
  public void deleteFromGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    throw unimplemented();
  }

  @Override
  public void deleteAllEntityGrantRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisBaseEntity lookupEntity(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId, int entityTypeCode) {
    throw useMetaStoreManager("lookupEntity/loadResolvedEntityById");
  }

  @Nullable
  @Override
  public PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int entityTypeCode,
      @Nonnull String name) {
    throw useMetaStoreManager("readEntityByName");
  }

  @Override
  public EntityNameLookupRecord lookupEntityIdAndSubTypeByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    throw unimplemented();
  }

  @Nonnull
  @Override
  public List<PolarisBaseEntity> lookupEntities(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    throw unimplemented();
  }

  @Nonnull
  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    throw unimplemented();
  }

  @Override
  public boolean hasChildren(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    throw unimplemented();
  }

  @Nonnull
  @Override
  public <T> Page<T> listFullEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer,
      PageToken pageToken) {
    throw useMetaStoreManager("listFullEntities");
  }

  @Nonnull
  @Override
  public Page<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull PageToken pageToken) {
    throw unimplemented();
  }

  @Override
  public void writeToPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    throw useMetaStoreManager("attachPolicyToEntity");
  }

  @Override
  public void deleteFromPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    throw useMetaStoreManager("detachPolicyFromEntity");
  }

  @Override
  public void deleteAllEntityPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisPolicyMappingRecord lookupPolicyMappingRecord(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    throw unimplemented();
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByType(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode) {
    throw useMetaStoreManager("loadPoliciesOnEntityByType");
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicy(
      @Nonnull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    LOGGER.warn(
        "loadAllTargetsOnPolicy is not implemented and always returns empty list, at least as long as org.apache.polaris.core.persistence.PolarisTestMetaStoreManager.testPolicyMappingCleanup calls this !!");
    return List.of();
    // throw useMetaStoreManager("loadEntitiesOnPolicy");
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @Nonnull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    throw useMetaStoreManager("loadPoliciesOnEntity");
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @Nullable
  @Override
  public PolarisGrantRecord lookupGrantRecord(
      @Nonnull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @Nonnull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @Nonnull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    throw useMetaStoreManager("loadGrantsToGrantee");
  }

  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          @Nonnull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    throw useMetaStoreManager("getSubscopedCredsForEntity");
  }

  @Override
  public void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    throw useMetaStoreManager("deletePrincipalSecrets");
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    throw useMetaStoreManager("rotatePrincipalSecrets");
  }

  @Nonnull
  @Override
  public PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets storePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      long principalId,
      @Nonnull String resolvedClientId,
      String customClientSecret) {
    throw useMetaStoreManager("storePrincipalSecrets");
  }

  @Override
  public void writeEvents(@Nonnull List<PolarisEvent> events) {
    throw unimplemented();
  }

  @Override
  public void deleteAll(@Nonnull PolarisCallContext callCtx) {
    throw unimplemented();
  }

  @Override
  public BasePersistence detach() {
    throw unimplemented();
  }

  static UnsupportedOperationException unimplemented() {
    var ex = new UnsupportedOperationException("Intentionally not implemented");
    LOGGER.error("Unsupported function call", ex);
    return ex;
  }

  static UnsupportedOperationException useMetaStoreManager(String function) {
    var ex =
        new UnsupportedOperationException(
            "Operation not supported - use PolarisMetaStoreManager." + function + "()");
    LOGGER.error("Unsupported function call", ex);
    return ex;
  }
}
