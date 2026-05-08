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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class NonFunctionalBasePersistence implements BasePersistence, IntegrationPersistence {
  private static final Logger LOGGER = LoggerFactory.getLogger(NonFunctionalBasePersistence.class);

  @Override
  public long generateNewId(@NonNull PolarisCallContext callCtx) {
    throw unimplemented();
  }

  @Override
  public void writeEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      // nameOrParentChanged is `true` if originalEntity==null or the parentId or the name changed
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    throw useMetaStoreManager("create/update/rename/delete");
  }

  @Override
  public void writeEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    throw useMetaStoreManager("create/update/rename/delete");
  }

  @Override
  public void writeToGrantRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
    throw unimplemented();
  }

  @Override
  public void deleteEntity(@NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity entity) {
    throw unimplemented();
  }

  @Override
  public void deleteFromGrantRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
    throw unimplemented();
  }

  @Override
  public void deleteAllEntityGrantRecords(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore entity,
      @NonNull List<PolarisGrantRecord> grantsOnGrantee,
      @NonNull List<PolarisGrantRecord> grantsOnSecurable) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisBaseEntity lookupEntity(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId, int entityTypeCode) {
    throw useMetaStoreManager("lookupEntity/loadResolvedEntityById");
  }

  @Nullable
  @Override
  public PolarisBaseEntity lookupEntityByName(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int entityTypeCode,
      @NonNull String name) {
    throw useMetaStoreManager("readEntityByName");
  }

  @Override
  public EntityNameLookupRecord lookupEntityIdAndSubTypeByName(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @NonNull String name) {
    throw unimplemented();
  }

  @NonNull
  @Override
  public List<PolarisBaseEntity> lookupEntities(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    throw unimplemented();
  }

  @NonNull
  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    throw unimplemented();
  }

  @Override
  public boolean hasChildren(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    throw unimplemented();
  }

  @NonNull
  @Override
  public <T> Page<T> listFullEntities(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull Predicate<PolarisBaseEntity> entityFilter,
      @NonNull Function<PolarisBaseEntity, T> transformer,
      PageToken pageToken) {
    throw useMetaStoreManager("listFullEntities");
  }

  @NonNull
  @Override
  public Page<EntityNameLookupRecord> listEntities(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    throw unimplemented();
  }

  @Override
  public void writeToPolicyMappingRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
    throw useMetaStoreManager("attachPolicyToEntity");
  }

  @Override
  public void deleteFromPolicyMappingRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
    throw useMetaStoreManager("detachPolicyFromEntity");
  }

  @Override
  public void deleteAllEntityPolicyMappingRecords(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      @NonNull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @NonNull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisPolicyMappingRecord lookupPolicyMappingRecord(
      @NonNull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode,
      long policyCatalogId,
      long policyId) {
    throw unimplemented();
  }

  @NonNull
  @Override
  public List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByType(
      @NonNull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode) {
    throw useMetaStoreManager("loadPoliciesOnEntityByType");
  }

  @NonNull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicy(
      @NonNull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    LOGGER.warn(
        "loadAllTargetsOnPolicy is not implemented and always returns empty list, at least as long as org.apache.polaris.core.persistence.PolarisTestMetaStoreManager.testPolicyMappingCleanup calls this !!");
    return List.of();
    // throw useMetaStoreManager("loadEntitiesOnPolicy");
  }

  @NonNull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @NonNull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    throw useMetaStoreManager("loadPoliciesOnEntity");
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @Nullable
  @Override
  public PolarisGrantRecord lookupGrantRecord(
      @NonNull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @NonNull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @NonNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    throw useMetaStoreManager("loadGrantsOnSecurable");
  }

  @NonNull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @NonNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    throw useMetaStoreManager("loadGrantsToGrantee");
  }

  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          @NonNull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity entity) {
    throw useMetaStoreManager("getSubscopedCredsForEntity");
  }

  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    throw useMetaStoreManager("deletePrincipalSecrets");
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets rotatePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash) {
    throw useMetaStoreManager("rotatePrincipalSecrets");
  }

  @NonNull
  @Override
  public PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String principalName, long principalId) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets loadPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
    throw unimplemented();
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets storePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      long principalId,
      @NonNull String resolvedClientId,
      String customClientSecret) {
    throw useMetaStoreManager("storePrincipalSecrets");
  }

  @Override
  public void writeEvents(@NonNull List<PolarisEvent> events) {
    throw unimplemented();
  }

  @Override
  public void deleteAll(@NonNull PolarisCallContext callCtx) {
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
