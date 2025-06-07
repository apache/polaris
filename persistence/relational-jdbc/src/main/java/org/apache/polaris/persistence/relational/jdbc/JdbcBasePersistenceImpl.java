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

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.polaris.core.policy.PolicyEntity;
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
      persistEntity(
          callCtx,
          entity,
          originalEntity,
          null,
          (connection, preparedQuery) -> {
            return datasourceOperations.executeUpdate(preparedQuery);
          });
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
          connection -> {
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
              persistEntity(
                  callCtx, entity, originalEntity, connection, datasourceOperations::execute);
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
      Connection connection,
      QueryAction queryAction)
      throws SQLException {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity, realmId);
    if (originalEntity == null) {
      try {
        List<Object> values =
            modelEntity.toMap(datasourceOperations.getDatabaseType()).values().stream().toList();
        queryAction.apply(connection, new PreparedQuery(SQLConstants.ENTITY_INSERT_QUERY, values));
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
      try {
        // object values
        List<Object> values =
            modelEntity.toMap(datasourceOperations.getDatabaseType()).values().stream().toList();
        // where clause params
        List<Object> whereClauseParams =
            List.of(
                originalEntity.getCatalogId(),
                originalEntity.getId(),
                originalEntity.getEntityVersion(),
                realmId);
        List<Object> mergedParams = new ArrayList<>(values); // order matters
        mergedParams.addAll(whereClauseParams);
        int rowsUpdated =
            queryAction.apply(
                connection, new PreparedQuery(SQLConstants.ENTITY_UPDATE_QUERY, mergedParams));
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
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec, realmId);
    try {
      List<Object> values =
          modelGrantRecord.toMap(datasourceOperations.getDatabaseType()).values().stream().toList();
      datasourceOperations.executeUpdate(
          new PreparedQuery(SQLConstants.GRANT_RECORD_INSERT_QUERY, values));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write to grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity, realmId);
    List<Object> params = List.of(realmId, modelEntity.getId());
    try {
      datasourceOperations.executeUpdate(
          new PreparedQuery(SQLConstants.ENTITY_DELETE_QUERY, params));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete entity due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteFromGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec, realmId);
    try {
      List<Object> params =
          modelGrantRecord.toMap(datasourceOperations.getDatabaseType()).values().stream().toList();
      datasourceOperations.executeUpdate(
          new PreparedQuery(SQLConstants.GRANT_RECORD_DELETE_QUERY, params));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete from grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteAllEntityGrantRecords(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {
    try {
      datasourceOperations.executeUpdate(
          new PreparedQuery(
              SQLConstants.GRANT_RECORD_DELETE_QUERY_FOR_ENTITY,
              List.of(
                  entity.getId(),
                  entity.getCatalogId(),
                  entity.getId(),
                  entity.getCatalogId(),
                  realmId)));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteAll(@Nonnull PolarisCallContext callCtx) {
    try {
      List<Object> params = List.of(realmId);
      datasourceOperations.runWithinTransaction(
          connection -> {
            datasourceOperations.execute(
                connection, new PreparedQuery(SQLConstants.ENTITY_DELETE_ALL_QUERY, params));
            datasourceOperations.execute(
                connection, new PreparedQuery(SQLConstants.GRANT_RECORD_DELETE_ALL_QUERY, params));
            datasourceOperations.execute(
                connection,
                new PreparedQuery(
                    SQLConstants.PRINCIPAL_AUTHENTICATION_DATA_DELETE_ALL_QUERY, params));
            datasourceOperations.execute(
                connection,
                new PreparedQuery(SQLConstants.POLICY_MAPPING_RECORD_DELETE_ALL_QUERY, params));
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
    // we just need to query by PK
    List<Object> params = List.of(catalogId, entityId, typeCode, realmId);
    return getPolarisBaseEntity(
        new PreparedQuery(SQLConstants.ENTITY_LOOKUP_BY_CATALOG_ID_ID_TYPE_CODE, params));
  }

  @Override
  public PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    List<Object> params = List.of(catalogId, parentId, typeCode, name, realmId);
    return getPolarisBaseEntity(
        new PreparedQuery(SQLConstants.ENTITY_LOOKUP_BY_NAME_QUERY, params));
  }

  @Nullable
  private PolarisBaseEntity getPolarisBaseEntity(PreparedQuery query) {
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
    String placeholders = entityIds.stream().map(e -> "(?, ?)").collect(Collectors.joining(", "));
    String sql = String.format(SQLConstants.ENTITY_LIST_BY_CATALOG_ID_AND_ID_QUERY, placeholders);

    List<Object> params =
        entityIds.stream()
            .flatMap(id -> Stream.of(id.getCatalogId(), id.getId()))
            .collect(Collectors.toCollection(ArrayList::new));
    params.add(realmId);
    try {
      return datasourceOperations.executeSelect(new PreparedQuery(sql, params), new ModelEntity());
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
                    entry -> ModelEntity.fromEntity(entry, realmId)));
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
    List<Object> params = List.of(catalogId, parentId, entityType.getCode(), realmId);
    try {
      PreparedQuery query = new PreparedQuery(SQLConstants.ENTITY_LIST_QUERY, params);
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
          results.stream().filter(entityFilter).map(transformer).collect(Collectors.toList());
      return Page.fromItems(resultsOrEmpty);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve polaris entities due to %s", e.getMessage()), e);
    }
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId) {
    List<Object> params = List.of(catalogId, entityId, realmId);
    PolarisBaseEntity b =
        getPolarisBaseEntity(
            new PreparedQuery(SQLConstants.ENTITY_LOOKUP_BY_CATALOG_ID_ID, params));
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
    try {
      List<Object> params =
          List.of(
              realmId, securableCatalogId, securableId, granteeCatalogId, granteeId, privilegeCode);
      var results =
          datasourceOperations.executeSelect(
              new PreparedQuery(SQLConstants.GRANT_RECORD_LOOKUP_BY_PK_QUERY, params),
              new ModelGrantRecord());
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
    List<Object> params = List.of(securableCatalogId, securableId, realmId);
    try {
      var results =
          datasourceOperations.executeSelect(
              new PreparedQuery(SQLConstants.GRANT_RECORD_LOOKUP_BY_SECURABLE_QUERY, params),
              new ModelGrantRecord());
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
    try {
      List<Object> params = List.of(granteeCatalogId, granteeId, realmId);
      var results =
          datasourceOperations.executeSelect(
              new PreparedQuery(SQLConstants.GRANT_RECORD_LOOKUP_BY_GRANTEE_QUERY, params),
              new ModelGrantRecord());
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
    var sql =
        new PreparedQuery(
            SQLConstants.ENTITY_LOOKUP_BY_PARENT_ID_QUERY, List.of(catalogId, parentId, realmId));
    if (optionalEntityType != null) {
      sql =
          new PreparedQuery(
              SQLConstants.ENTITY_LOOKUP_BY_PARENT_ID_WITH_TYPE_CODE_QUERY,
              List.of(catalogId, parentId, optionalEntityType.getCode(), realmId));
    }
    try {
      var results = datasourceOperations.executeSelect(sql, new ModelEntity());
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
    try {
      var results =
          datasourceOperations.executeSelect(
              new PreparedQuery(
                  SQLConstants.PRINCIPAL_AUTHENTICATION_DATA_LOOKUP_BY_PRIMARY_KET_QUERY,
                  List.of(realmId, clientId)),
              new ModelPrincipalAuthenticationData());
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
              loadPrincipalSecrets(callCtx, principalSecrets.getPrincipalClientId()), realmId);
    } while (lookupPrincipalSecrets != null);

    lookupPrincipalSecrets =
        ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(principalSecrets, realmId);

    // write new principal secrets
    try {
      List<Object> values =
          lookupPrincipalSecrets.toMap(datasourceOperations.getDatabaseType()).values().stream()
              .toList();
      datasourceOperations.executeUpdate(
          new PreparedQuery(SQLConstants.PRINCIPAL_AUTHENTICATION_DATA_INSERT_QUERY, values));
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
    try {
      ModelPrincipalAuthenticationData modelPrincipalAuthenticationData =
          ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(
              principalSecrets, realmId);
      List<Object> params = List.of(realmId, clientId);
      List<Object> values =
          modelPrincipalAuthenticationData
              .toMap(datasourceOperations.getDatabaseType())
              .values()
              .stream()
              .toList();
      List<Object> mergedList = new ArrayList<>(values);
      mergedList.addAll(params);
      datasourceOperations.executeUpdate(
          new PreparedQuery(SQLConstants.PRINCIPAL_AUTHENTICATION_DATA_UPDATE_QUERY, mergedList));
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
    try {
      List<Object> params = List.of(realmId, clientId, principalId);
      datasourceOperations.executeUpdate(
          new PreparedQuery(SQLConstants.PRINCIPAL_AUTHENTICATION_DELETE_QUERY, params));
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
          connection -> {
            PolicyType policyType = PolicyType.fromCode(record.getPolicyTypeCode());
            Preconditions.checkArgument(
                policyType != null, "Invalid policy type code: %s", record.getPolicyTypeCode());
            ModelPolicyMappingRecord modelPolicyMappingRecord =
                ModelPolicyMappingRecord.fromPolicyMappingRecord(record, realmId);
            List<Object> values =
                modelPolicyMappingRecord
                    .toMap(datasourceOperations.getDatabaseType())
                    .values()
                    .stream()
                    .toList();
            PreparedQuery insertPolicyMappingQuery =
                new PreparedQuery(SQLConstants.POLICY_MAPPING_INSERT_QUERY, values);
            if (policyType.isInheritable()) {
              return handleInheritablePolicy(callCtx, record, insertPolicyMappingQuery, connection);
            } else {
              datasourceOperations.execute(connection, insertPolicyMappingQuery);
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
      @Nonnull PreparedQuery insertQuery,
      Connection connection)
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
      // In case of the mapping exist, update the policy mapping with the new parameters.
      ModelPolicyMappingRecord modelPolicyMappingRecord =
          ModelPolicyMappingRecord.fromPolicyMappingRecord(record, realmId);

      List<Object> params =
          modelPolicyMappingRecord.toMap(datasourceOperations.getDatabaseType()).values().stream()
              .toList();
      List<Object> updateClauseParams =
          List.of(
              realmId,
              record.getTargetCatalogId(),
              record.getTargetId(),
              record.getPolicyTypeCode(),
              record.getPolicyCatalogId(),
              record.getPolicyId());
      List<Object> finalParams = new ArrayList<>(params);
      finalParams.addAll(updateClauseParams);
      PreparedQuery updateQuery =
          new PreparedQuery(SQLConstants.POLICY_MAPPING_UPDATE_QUERY, finalParams);
      datasourceOperations.execute(connection, updateQuery);
    } else {
      // record doesn't exist do an insert.
      datasourceOperations.executeUpdate(insertQuery);
    }
    return true;
  }

  @Override
  public void deleteFromPolicyMappingRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisPolicyMappingRecord record) {
    var modelPolicyMappingRecord =
        ModelPolicyMappingRecord.fromPolicyMappingRecord(record, realmId);
    try {
      List<Object> params =
          modelPolicyMappingRecord.toMap(datasourceOperations.getDatabaseType()).values().stream()
              .toList();
      datasourceOperations.executeUpdate(
          new PreparedQuery(SQLConstants.POLICY_MAPPING_RECORD_DELETE_QUERY, params));
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
      PreparedQuery query;
      List<Object> params;
      if (entity.getType() == PolarisEntityType.POLICY) {
        PolicyEntity policyEntity = PolicyEntity.of(entity);
        params =
            List.of(
                policyEntity.getPolicyTypeCode(),
                policyEntity.getCatalogId(),
                policyEntity.getId(),
                realmId);
        query = new PreparedQuery(SQLConstants.POLICY_MAPPING_RECORD_DELETE_ENTITY_QUERY, params);
      } else {
        params = List.of(entity.getCatalogId(), entity.getId(), realmId);
        query =
            new PreparedQuery(SQLConstants.POLICY_MAPPING_RECORD_DELETE_ENTITY_BY_ID_QUERY, params);
      }
      datasourceOperations.executeUpdate(query);
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
    List<Object> params =
        List.of(realmId, targetCatalogId, targetId, policyTypeCode, policyCatalogId, policyId);
    List<PolarisPolicyMappingRecord> results =
        fetchPolicyMappingRecords(
            new PreparedQuery(
                SQLConstants.POLICY_MAPPING_RECORD_LOOKUP_BY_PRIMARY_KEY_QUERY, params));
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
    List<Object> params = List.of(targetCatalogId, targetId, policyTypeCode, realmId);
    return fetchPolicyMappingRecords(
        new PreparedQuery(SQLConstants.POLICY_MAPPING_RECORD_LOOKUP_BY_TYPE_QUERY, params));
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @Nonnull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    List<Object> params = List.of(targetCatalogId, targetId, realmId);
    return fetchPolicyMappingRecords(
        new PreparedQuery(SQLConstants.POLICY_MAPPING_RECORD_LOOKUP_ON_TARGET_QUERY, params));
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicy(
      @Nonnull PolarisCallContext callCtx,
      long policyCatalogId,
      long policyId,
      int policyTypeCode) {
    List<Object> params = List.of(policyTypeCode, policyCatalogId, policyId, realmId);
    return fetchPolicyMappingRecords(
        new PreparedQuery(
            SQLConstants.POLICY_MAPPING_RECORD_LOOKUP_TARGETS_ON_POLICY_QUERY, params));
  }

  private List<PolarisPolicyMappingRecord> fetchPolicyMappingRecords(PreparedQuery query) {
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
    Integer apply(Connection connection, PreparedQuery query) throws SQLException;
  }
}
