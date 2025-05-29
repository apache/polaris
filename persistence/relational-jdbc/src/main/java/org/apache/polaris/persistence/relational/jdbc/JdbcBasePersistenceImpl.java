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
package org.apache.polaris.persistence.relational.jdbc;

import static org.apache.polaris.persistence.relational.jdbc.QueryGenerator.*;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.pagination.HasPageSize;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPolicyMappingRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcBasePersistenceImpl implements BasePersistence, IntegrationPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBasePersistenceImpl.class);

  private final DatasourceOperations datasourceOperations;
  private final PrincipalSecretsGenerator secretsGenerator;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final String realmId;

  public JdbcBasePersistenceImpl(
      DatasourceOperations databaseOperations,
      PrincipalSecretsGenerator secretsGenerator,
      PolarisStorageIntegrationProvider storageIntegrationProvider,
      String realmId) {
    this.datasourceOperations = databaseOperations;
    this.secretsGenerator = secretsGenerator;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.realmId = realmId;
  }

  @Override
  public long generateNewId(@Nonnull PolarisCallContext callCtx) {
    return IdGenerator.getIdGenerator().nextId();
  }

  @Override
  public void writeEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      PolarisBaseEntity originalEntity) {
    try {
      persistEntity(callCtx, entity, originalEntity, datasourceOperations::executeUpdate);
    } catch (SQLException e) {
      throw new RuntimeException("Error persisting entity", e);
    }
  }

  @Override
  public void writeEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      List<PolarisBaseEntity> originalEntities) {
    try {
      datasourceOperations.runWithinTransaction(
          statement -> {
            for (int i = 0; i < entities.size(); i++) {
              PolarisBaseEntity entity = entities.get(i);
              PolarisBaseEntity originalEntity =
                  originalEntities != null ? originalEntities.get(i) : null;
              // first, check if the entity has already been created, in which case we will simply
              // return it.
              PolarisBaseEntity entityFound =
                  lookupEntity(
                      callCtx, entity.getCatalogId(), entity.getId(), entity.getTypeCode());
              if (entityFound != null && originalEntity == null) {
                // probably the client retried, simply return it
                // TODO: Check correctness of returning entityFound vs entity here. It may have
                // already been updated after the creation.
                continue;
              }
              persistEntity(callCtx, entity, originalEntity, statement::executeUpdate);
            }
            return true;
          });
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Error executing the transaction for writing entities due to %s", e.getMessage()),
          e);
    }
  }

  private void persistEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      PolarisBaseEntity originalEntity,
      QueryAction queryAction)
      throws SQLException {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity);
    if (originalEntity == null) {
      try {
        queryAction.apply(generateInsertQuery(modelEntity, realmId));
      } catch (SQLException e) {
        if (datasourceOperations.isConstraintViolation(e)) {
          PolarisBaseEntity existingEntity =
              lookupEntityByName(
                  callCtx,
                  entity.getCatalogId(),
                  entity.getParentId(),
                  entity.getTypeCode(),
                  entity.getName());
          throw new EntityAlreadyExistsException(existingEntity, e);
        } else {
          throw new RuntimeException(
              String.format("Failed to write entity due to %s", e.getMessage()), e);
        }
      }
    } else {
      Map<String, Object> params =
          Map.of(
              "id",
              originalEntity.getId(),
              "catalog_id",
              originalEntity.getCatalogId(),
              "entity_version",
              originalEntity.getEntityVersion(),
              "realm_id",
              realmId);
      try {
        int rowsUpdated = queryAction.apply(generateUpdateQuery(modelEntity, params));
        if (rowsUpdated == 0) {
          throw new RetryOnConcurrencyException(
              "Entity '%s' id '%s' concurrently modified; expected version %s",
              originalEntity.getName(), originalEntity.getId(), originalEntity.getEntityVersion());
        }
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format("Failed to write entity due to %s", e.getMessage()), e);
      }
    }
  }

  @Override
  public void writeToGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec);
    String query = generateInsertQuery(modelGrantRecord, realmId);
    try {
      datasourceOperations.executeUpdate(query);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write to grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity);
    Map<String, Object> params =
        Map.of(
            "id",
            modelEntity.getId(),
            "catalog_id",
            modelEntity.getCatalogId(),
            "realm_id",
            realmId);
    try {
      datasourceOperations.executeUpdate(generateDeleteQuery(ModelEntity.class, params));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete entity due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteFromGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec);
    String query = generateDeleteQuery(modelGrantRecord, realmId);
    try {
      datasourceOperations.executeUpdate(query);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete from grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteAllEntityGrantRecords(
      @Nonnull PolarisCallContext callCtx,
      PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {
    try {
      datasourceOperations.executeUpdate(generateDeleteQueryForEntityGrantRecords(entity, realmId));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteAll(@Nonnull PolarisCallContext callCtx) {
    try {
      datasourceOperations.runWithinTransaction(
          statement -> {
            statement.executeUpdate(generateDeleteAll(ModelEntity.class, realmId));
            statement.executeUpdate(generateDeleteAll(ModelGrantRecord.class, realmId));
            statement.executeUpdate(
                generateDeleteAll(ModelPrincipalAuthenticationData.class, realmId));
            statement.executeUpdate(generateDeleteAll(ModelPolicyMappingRecord.class, realmId));
            return true;
          });
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete all due to %s", e.getMessage()), e);
    }
  }

  @Override
  public PolarisBaseEntity lookupEntity(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    Map<String, Object> params =
        Map.of("catalog_id", catalogId, "id", entityId, "type_code", typeCode, "realm_id", realmId);
    String query = generateSelectQuery(new ModelEntity(), params);
    return getPolarisBaseEntity(query);
  }

  @Override
  public PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    Map<String, Object> params =
        Map.of(
            "catalog_id",
            catalogId,
            "parent_id",
            parentId,
            "type_code",
            typeCode,
            "name",
            name,
            "realm_id",
            realmId);
    String query = generateSelectQuery(new ModelEntity(), params);
    return getPolarisBaseEntity(query);
  }

  @Nullable
  private PolarisBaseEntity getPolarisBaseEntity(String query) {
    try {
      var results = datasourceOperations.executeSelect(query, new ModelEntity());
      if (results.isEmpty()) {
        return null;
      } else if (results.size() > 1) {
        throw new IllegalStateException(
            String.format(
                "More than one(%s) entities were found for a given type code : %s",
                results.size(), results.getFirst().getTypeCode()));
      } else {
        return results.getFirst();
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve polaris entity due to %s", e.getMessage()), e);
    }
  }

  @Nonnull
  @Override
  public List<PolarisBaseEntity> lookupEntities(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    if (entityIds == null || entityIds.isEmpty()) return new ArrayList<>();
    String query = generateSelectQueryWithEntityIds(realmId, entityIds);
    try {
      return datasourceOperations.executeSelect(query, new ModelEntity());
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve polaris entities due to %s", e.getMessage()), e);
    }
  }

  @Nonnull
  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    Map<PolarisEntityId, ModelEntity> idToEntityMap =
        lookupEntities(callCtx, entityIds).stream()
            .collect(
                Collectors.toMap(
                    entry -> new PolarisEntityId(entry.getCatalogId(), entry.getId()),
                    ModelEntity::fromEntity));
    return entityIds.stream()
        .map(
            entityId -> {
              ModelEntity entity = idToEntityMap.getOrDefault(entityId, null);
              return entity == null
                  ? null
                  : new PolarisChangeTrackingVersions(
                      entity.getEntityVersion(), entity.getGrantRecordsVersion());
            })
        .collect(Collectors.toList());
  }

  @Nonnull
  @Override
  public Page<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PageToken pageToken) {
    return listEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        entity -> true,
        EntityNameLookupRecord::new,
        pageToken);
  }

  @Nonnull
  @Override
  public Page<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull PageToken pageToken) {
    return listEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        entityFilter,
        EntityNameLookupRecord::new,
        pageToken);
  }

  @Nonnull
  @Override
  public <T> Page<T> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer,
      @Nonnull PageToken pageToken) {
    Map<String, Object> params =
        Map.of(
            "catalog_id",
            catalogId,
            "parent_id",
            parentId,
            "type_code",
            entityType.getCode(),
            "realm_id",
            realmId);

    // Limit can't be pushed down, due to client side filtering
    // absence of transaction.
    String query = QueryGenerator.generateSelectQuery(new ModelEntity(), params);
    try {
      List<PolarisBaseEntity> results = new ArrayList<>();
      datasourceOperations.executeSelectOverStream(
          query,
          new ModelEntity(),
          stream -> {
            var data = stream.filter(entityFilter);
            if (pageToken instanceof HasPageSize hasPageSize) {
              data = data.limit(hasPageSize.getPageSize());
            }
            data.forEach(results::add);
          });
      List<T> resultsOrEmpty =
          results == null
              ? Collections.emptyList()
              : results.stream().filter(entityFilter).map(transformer).collect(Collectors.toList());
      return Page.fromItems(resultsOrEmpty);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve polaris entities due to %s", e.getMessage()), e);
    }
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId) {

    Map<String, Object> params =
        Map.of("catalog_id", catalogId, "id", entityId, "realm_id", realmId);
    String query = QueryGenerator.generateSelectQuery(new ModelEntity(), params);
    PolarisBaseEntity b = getPolarisBaseEntity(query);
    return b == null ? 0 : b.getGrantRecordsVersion();
  }

  @Override
  public PolarisGrantRecord lookupGrantRecord(
      @Nonnull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    Map<String, Object> params =
        Map.of(
            "securable_catalog_id",
            securableCatalogId,
            "securable_id",
            securableId,
            "grantee_catalog_id",
            granteeCatalogId,
            "grantee_id",
            granteeId,
            "privilege_code",
            privilegeCode,
            "realm_id",
            realmId);
    String query = generateSelectQuery(new ModelGrantRecord(), params);
    try {
      var results = datasourceOperations.executeSelect(query, new ModelGrantRecord());
      if (results.size() > 1) {
        throw new IllegalStateException(
            String.format(
                "More than one grant record %s for a given Grant record", results.getFirst()));
      } else if (results.isEmpty()) {
        return null;
      }
      return results.getFirst();
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve grant record due to %s", e.getMessage()), e);
    }
  }

  @Nonnull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    Map<String, Object> params =
        Map.of(
            "securable_catalog_id",
            securableCatalogId,
            "securable_id",
            securableId,
            "realm_id",
            realmId);
    String query = generateSelectQuery(new ModelGrantRecord(), params);
    try {
      var results = datasourceOperations.executeSelect(query, new ModelGrantRecord());
      return results == null ? Collections.emptyList() : results;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve grant records for securableCatalogId: %s securableId: %s due to %s",
              securableCatalogId, securableId, e.getMessage()),
          e);
    }
  }

  @Nonnull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    Map<String, Object> params =
        Map.of(
            "grantee_catalog_id", granteeCatalogId, "grantee_id", granteeId, "realm_id", realmId);
    String query = generateSelectQuery(new ModelGrantRecord(), params);
    try {
      var results = datasourceOperations.executeSelect(query, new ModelGrantRecord());
      return results == null ? Collections.emptyList() : results;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve grant records for granteeCatalogId: %s granteeId: %s due to %s",
              granteeCatalogId, granteeId, e.getMessage()),
          e);
    }
  }

  @Override
  public boolean hasChildren(
      @Nonnull PolarisCallContext callContext,
      PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    Map<String, Object> params = new HashMap<>();
    params.put("realm_id", realmId);
    params.put("catalog_id", catalogId);
    params.put("parent_id", parentId);
    if (optionalEntityType != null) {
      params.put("type_code", optionalEntityType.getCode());
    }
    String query = generateSelectQuery(new ModelEntity(), params);
    try {
      var results = datasourceOperations.executeSelect(query, new ModelEntity());
      return results != null && !results.isEmpty();
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve entities for catalogId: %s due to %s", catalogId, e.getMessage()),
          e);
    }
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    Map<String, Object> params = Map.of("principal_client_id", clientId, "realm_id", realmId);
    String query = generateSelectQuery(new ModelPrincipalAuthenticationData(), params);
    try {
      var results =
          datasourceOperations.executeSelect(query, new ModelPrincipalAuthenticationData());
      return results == null || results.isEmpty() ? null : results.getFirst();
    } catch (SQLException e) {
      LOGGER.error(
          "Failed to retrieve principals secrets for client id: {}, due to {}",
          clientId,
          e.getMessage(),
          e);
      throw new RuntimeException(
          String.format("Failed to retrieve principal secrets for clientId: %s", clientId), e);
    }
  }

  @Nonnull
  @Override
  public PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId) {
    // ensure principal client id is unique
    PolarisPrincipalSecrets principalSecrets;
    ModelPrincipalAuthenticationData lookupPrincipalSecrets;
    do {
      // generate new random client id and secrets
      principalSecrets = secretsGenerator.produceSecrets(principalName, principalId);

      // load the existing secrets
      lookupPrincipalSecrets =
          ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(
              loadPrincipalSecrets(callCtx, principalSecrets.getPrincipalClientId()));
    } while (lookupPrincipalSecrets != null);

    lookupPrincipalSecrets =
        ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(principalSecrets);

    // write new principal secrets
    String query = generateInsertQuery(lookupPrincipalSecrets, realmId);
    try {
      datasourceOperations.executeUpdate(query);
    } catch (SQLException e) {
      LOGGER.error(
          "Failed to generate new principal secrets for principalId: {}, due to {}",
          principalId,
          e.getMessage(),
          e);
      throw new RuntimeException(
          String.format(
              "Failed to generate new principal secrets for principalId: %s", principalId),
          e);
    }
    // if not found, return null
    return principalSecrets;
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    // load the existing secrets
    PolarisPrincipalSecrets principalSecrets = loadPrincipalSecrets(callCtx, clientId);

    // should be found
    callCtx
        .getDiagServices()
        .checkNotNull(
            principalSecrets,
            "cannot_find_secrets",
            "client_id={} principalId={}",
            clientId,
            principalId);

    // ensure principal id is matching
    callCtx
        .getDiagServices()
        .check(
            principalId == principalSecrets.getPrincipalId(),
            "principal_id_mismatch",
            "expectedId={} id={}",
            principalId,
            principalSecrets.getPrincipalId());

    // rotate the secrets
    principalSecrets.rotateSecrets(oldSecretHash);
    if (reset) {
      principalSecrets.rotateSecrets(principalSecrets.getMainSecretHash());
    }

    Map<String, Object> params = Map.of("principal_client_id", clientId, "realm_id", realmId);
    // write back new secrets
    String query =
        generateUpdateQuery(
            ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(principalSecrets),
            params);
    try {
      datasourceOperations.executeUpdate(query);
    } catch (SQLException e) {
      LOGGER.error(
          "Failed to rotatePrincipalSecrets  for clientId: {}, due to {}",
          clientId,
          e.getMessage(),
          e);
      throw new RuntimeException(
          String.format("Failed to rotatePrincipalSecrets for clientId: %s", clientId), e);
    }

    // return those
    return principalSecrets;
  }

  @Override
  public void deletePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    Map<String, Object> params =
        Map.of("principal_client_id", clientId, "principal_id", principalId, "realm_id", realmId);
    String query = generateDeleteQuery(ModelPrincipalAuthenticationData.class, params);
    try {
      datasourceOperations.executeUpdate(query);
    } catch (SQLException e) {
      LOGGER.error(
          "Failed to delete principalSecrets for clientId: {}, due to {}",
          clientId,
          e.getMessage(),
          e);
      throw new RuntimeException(
          String.format("Failed to delete principalSecrets for clientId: %s", clientId), e);
    }
  }

  @Override
  public void writeToPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    try {
      datasourceOperations.runWithinTransaction(
          statement -> {
            PolicyType policyType = PolicyType.fromCode(record.getPolicyTypeCode());
            Preconditions.checkArgument(
                policyType != null, "Invalid policy type code: %s", record.getPolicyTypeCode());
            String insertPolicyMappingQuery =
                generateInsertQuery(
                    ModelPolicyMappingRecord.fromPolicyMappingRecord(record), realmId);
            if (policyType.isInheritable()) {
              return handleInheritablePolicy(callCtx, record, insertPolicyMappingQuery, statement);
            } else {
              statement.executeUpdate(insertPolicyMappingQuery);
            }
            return true;
          });
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write to policy mapping records due to %s", e.getMessage()), e);
    }
  }

  private boolean handleInheritablePolicy(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisPolicyMappingRecord record,
      @Nonnull String insertQuery,
      Statement statement)
      throws SQLException {
    List<PolarisPolicyMappingRecord> existingRecords =
        loadPoliciesOnTargetByType(
            callCtx, record.getTargetCatalogId(), record.getTargetId(), record.getPolicyTypeCode());
    if (existingRecords.size() > 1) {
      throw new PolicyMappingAlreadyExistsException(existingRecords.getFirst());
    } else if (existingRecords.size() == 1) {
      PolarisPolicyMappingRecord existingRecord = existingRecords.getFirst();
      if (existingRecord.getPolicyCatalogId() != record.getPolicyCatalogId()
          || existingRecord.getPolicyId() != record.getPolicyId()) {
        // Only one policy of the same type can be attached to an entity when the policy is
        // inheritable.
        throw new PolicyMappingAlreadyExistsException(existingRecord);
      }
      Map<String, Object> updateClause =
          Map.of(
              "target_catalog_id",
              record.getTargetCatalogId(),
              "target_id",
              record.getTargetId(),
              "policy_type_code",
              record.getPolicyTypeCode(),
              "policy_id",
              record.getPolicyId(),
              "policy_catalog_id",
              record.getPolicyCatalogId(),
              "realm_id",
              realmId);
      // In case of the mapping exist, update the policy mapping with the new parameters.
      String updateQuery =
          generateUpdateQuery(
              ModelPolicyMappingRecord.fromPolicyMappingRecord(record), updateClause);
      statement.executeUpdate(updateQuery);
    } else {
      // record doesn't exist do an insert.
      statement.executeUpdate(insertQuery);
    }
    return true;
  }

  @Override
  public void deleteFromPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    var modelPolicyMappingRecord = ModelPolicyMappingRecord.fromPolicyMappingRecord(record);
    String query = generateDeleteQuery(modelPolicyMappingRecord, realmId);
    try {
      datasourceOperations.executeUpdate(query);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write to policy records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteAllEntityPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @Nonnull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
    try {
      datasourceOperations.executeUpdate(
          generateDeleteQueryForEntityPolicyMappingRecords(entity, realmId));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete policy mapping records due to %s", e.getMessage()), e);
    }
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
    Map<String, Object> params =
        Map.of(
            "target_catalog_id",
            targetCatalogId,
            "target_id",
            targetId,
            "policy_type_code",
            policyTypeCode,
            "policy_id",
            policyId,
            "policy_catalog_id",
            policyCatalogId,
            "realm_id",
            realmId);
    String query = generateSelectQuery(new ModelPolicyMappingRecord(), params);
    List<PolarisPolicyMappingRecord> results = fetchPolicyMappingRecords(query);
    Preconditions.checkState(results.size() <= 1, "More than one policy mapping records found");
    return results.size() == 1 ? results.getFirst() : null;
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByType(
      @Nonnull PolarisCallContext callCtx,
      long targetCatalogId,
      long targetId,
      int policyTypeCode) {
    Map<String, Object> params =
        Map.of(
            "target_catalog_id",
            targetCatalogId,
            "target_id",
            targetId,
            "policy_type_code",
            policyTypeCode,
            "realm_id",
            realmId);
    String query = generateSelectQuery(new ModelPolicyMappingRecord(), params);
    return fetchPolicyMappingRecords(query);
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @Nonnull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    Map<String, Object> params =
        Map.of("target_catalog_id", targetCatalogId, "target_id", targetId, "realm_id", realmId);
    String query = generateSelectQuery(new ModelPolicyMappingRecord(), params);
    return fetchPolicyMappingRecords(query);
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicy(
      @Nonnull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    Map<String, Object> params =
        Map.of(
            "policy_type_code",
            policyTypeCode,
            "policy_catalog_id",
            policyCatalogId,
            "policy_id",
            policyId,
            "realm_id",
            realmId);
    String query = generateSelectQuery(new ModelPolicyMappingRecord(), params);
    return fetchPolicyMappingRecords(query);
  }

  private List<PolarisPolicyMappingRecord> fetchPolicyMappingRecords(String query) {
    try {
      var results = datasourceOperations.executeSelect(query, new ModelPolicyMappingRecord());
      return results == null ? Collections.emptyList() : results;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve policy mapping records %s", e.getMessage()), e);
    }
  }

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          @Nonnull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return storageIntegrationProvider.getStorageIntegrationForConfig(
        polarisStorageConfigurationInfo);
  }

  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      @Nonnull PolarisCallContext callContext,
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration<T> storageIntegration) {}

  @Nullable
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          @Nonnull PolarisCallContext callContext, @Nonnull PolarisBaseEntity entity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(callContext, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  @FunctionalInterface
  private interface QueryAction {
    Integer apply(String query) throws SQLException;
  }
}
