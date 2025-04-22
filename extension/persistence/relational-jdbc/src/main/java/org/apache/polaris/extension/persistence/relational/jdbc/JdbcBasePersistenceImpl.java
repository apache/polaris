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
package org.apache.polaris.extension.persistence.relational.jdbc;

import static org.apache.polaris.extension.persistence.relational.jdbc.QueryGenerator.*;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
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
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.extension.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.extension.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.extension.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;
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
    ModelEntity modelEntity = ModelEntity.fromEntity(entity);
    String query;
    if (originalEntity == null) {
      try {
        query = generateInsertQuery(modelEntity, realmId);
        datasourceOperations.executeUpdate(query);
      } catch (SQLException e) {
        if ((datasourceOperations.isConstraintViolation(e)
            || datasourceOperations.isAlreadyExistsException(e))) {
          EntityAlreadyExistsException ee = new EntityAlreadyExistsException(entity);
          ee.initCause(e);
          throw ee;
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
      query = generateUpdateQuery(modelEntity, params);
      try {
        int rowsUpdated = datasourceOperations.executeUpdate(query);
        if (rowsUpdated == 0) {
          throw new RetryOnConcurrencyException(
              "Entity '%s' id '%s' concurrently modified; expected version %s",
              entity.getName(), entity.getId(), originalEntity.getEntityVersion());
        }
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format("Failed to write entity due to %s", e.getMessage()), e);
      }
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
              ModelEntity modelEntity = ModelEntity.fromEntity(entity);

              // first, check if the entity has already been created, in which case we will simply
              // return it.
              PolarisBaseEntity entityFound =
                  lookupEntity(
                      callCtx, entity.getCatalogId(), entity.getId(), entity.getTypeCode());
              if (entityFound != null) {
                // probably the client retried, simply return it
                // TODO: Check correctness of returning entityFound vs entity here. It may have
                // already been updated after the creation.
                continue;
              }
              // lookup by name
              EntityNameLookupRecord exists =
                  lookupEntityIdAndSubTypeByName(
                      callCtx,
                      entity.getCatalogId(),
                      entity.getParentId(),
                      entity.getTypeCode(),
                      entity.getName());
              if (exists != null) {
                throw new EntityAlreadyExistsException(entity);
              }
              String query;
              if (originalEntities == null || originalEntities.get(i) == null) {
                try {
                  query = generateInsertQuery(modelEntity, realmId);
                  statement.executeUpdate(query);
                } catch (SQLException e) {
                  if ((datasourceOperations.isConstraintViolation(e)
                      || datasourceOperations.isAlreadyExistsException(e))) {
                    EntityAlreadyExistsException ee = new EntityAlreadyExistsException(entity);
                    ee.initCause(e);
                    throw ee;
                  } else {
                    throw new RuntimeException(
                        String.format("Failed to write entity due to %s", e.getMessage()), e);
                  }
                }
              } else {
                Map<String, Object> params =
                    Map.of(
                        "id",
                        originalEntities.get(i).getId(),
                        "catalog_id",
                        originalEntities.get(i).getCatalogId(),
                        "entity_version",
                        originalEntities.get(i).getEntityVersion(),
                        "realm_id",
                        realmId);
                query = generateUpdateQuery(modelEntity, params);
                try {
                  int rowsUpdated = statement.executeUpdate(query);
                  if (rowsUpdated == 0) {
                    throw new RetryOnConcurrencyException(
                        "Entity '%s' id '%s' concurrently modified; expected version %s",
                        entity.getName(),
                        entity.getId(),
                        originalEntities.get(i).getEntityVersion());
                  }
                } catch (SQLException e) {
                  throw new RuntimeException(
                      String.format("Failed to write entity due to %s", e.getMessage()), e);
                }
              }
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
    String query = generateDeleteQuery(modelGrantRecord, ModelGrantRecord.class, realmId);
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
      datasourceOperations.executeUpdate(generateDeleteAll(ModelEntity.class, realmId));
      datasourceOperations.executeUpdate(generateDeleteAll(ModelGrantRecord.class, realmId));
      datasourceOperations.executeUpdate(generateDeleteAll(ModelEntity.class, realmId));
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
    String query = generateSelectQuery(ModelEntity.class, params);
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
    String query = generateSelectQuery(ModelEntity.class, params);
    return getPolarisBaseEntity(query);
  }

  @Nullable
  private PolarisBaseEntity getPolarisBaseEntity(String query) {
    try {
      List<PolarisBaseEntity> results =
          datasourceOperations.executeSelect(
              query, ModelEntity.class, ModelEntity::toEntity, null, Integer.MAX_VALUE);
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
      return datasourceOperations.executeSelect(
          query, ModelEntity.class, ModelEntity::toEntity, null, Integer.MAX_VALUE);
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
  public List<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType) {
    return listEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        Integer.MAX_VALUE,
        entity -> true,
        EntityNameLookupRecord::new);
  }

  @Nonnull
  @Override
  public List<EntityNameLookupRecord> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter) {
    return listEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        Integer.MAX_VALUE,
        entityFilter,
        EntityNameLookupRecord::new);
  }

  @Nonnull
  @Override
  public <T> List<T> listEntities(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      int limit,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer) {
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
    String query = QueryGenerator.generateSelectQuery(ModelEntity.class, params);
    try {
      List<PolarisBaseEntity> results =
          datasourceOperations.executeSelect(
              query, ModelEntity.class, ModelEntity::toEntity, entityFilter, limit);
      return results == null
          ? Collections.emptyList()
          : results.stream().filter(entityFilter).map(transformer).collect(Collectors.toList());
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
    String query = QueryGenerator.generateSelectQuery(ModelEntity.class, params);
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
    String query = generateSelectQuery(ModelGrantRecord.class, params);
    try {
      List<PolarisGrantRecord> results =
          datasourceOperations.executeSelect(
              query,
              ModelGrantRecord.class,
              ModelGrantRecord::toGrantRecord,
              null,
              Integer.MAX_VALUE);
      if (results.size() > 1) {
        throw new IllegalStateException(
            String.format(
                "More than one grant record %s for a given Grant record", results.getFirst()));
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
    String query = generateSelectQuery(ModelGrantRecord.class, params);
    try {
      List<PolarisGrantRecord> results =
          datasourceOperations.executeSelect(
              query,
              ModelGrantRecord.class,
              ModelGrantRecord::toGrantRecord,
              null,
              Integer.MAX_VALUE);
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
    String query = generateSelectQuery(ModelGrantRecord.class, params);
    try {
      List<PolarisGrantRecord> results =
          datasourceOperations.executeSelect(
              query,
              ModelGrantRecord.class,
              ModelGrantRecord::toGrantRecord,
              null,
              Integer.MAX_VALUE);
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
    String query = generateSelectQuery(ModelEntity.class, params);
    try {
      List<ModelEntity> results =
          datasourceOperations.executeSelect(
              query, ModelEntity.class, Function.identity(), null, Integer.MAX_VALUE);
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
    String query = generateSelectQuery(ModelPrincipalAuthenticationData.class, params);
    try {
      List<PolarisPrincipalSecrets> results =
          datasourceOperations.executeSelect(
              query,
              ModelPrincipalAuthenticationData.class,
              ModelPrincipalAuthenticationData::toPrincipalAuthenticationData,
              null,
              Integer.MAX_VALUE);
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
}
