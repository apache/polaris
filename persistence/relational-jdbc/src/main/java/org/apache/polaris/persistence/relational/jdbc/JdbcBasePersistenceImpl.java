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

import static org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.IdempotencyRecord;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.IdempotencyPersistenceException;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.ScanMetricsRecord;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.persistence.relational.jdbc.models.EntityNameLookupRecordConverter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEvent;
import org.apache.polaris.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelIdempotencyRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPolicyMappingRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.SchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcBasePersistenceImpl implements BasePersistence, IntegrationPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBasePersistenceImpl.class);

  private final PolarisDiagnostics diagnostics;
  private final DatasourceOperations datasourceOperations;
  private final PrincipalSecretsGenerator secretsGenerator;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final String realmId;
  private final int schemaVersion;

  // The max number of components a location can have before the optimized sibling check is not used
  private static final int MAX_LOCATION_COMPONENTS = 40;

  public JdbcBasePersistenceImpl(
      PolarisDiagnostics diagnostics,
      DatasourceOperations databaseOperations,
      PrincipalSecretsGenerator secretsGenerator,
      PolarisStorageIntegrationProvider storageIntegrationProvider,
      String realmId,
      int schemaVersion) {
    this.diagnostics = diagnostics;
    this.datasourceOperations = databaseOperations;
    this.secretsGenerator = secretsGenerator;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.realmId = realmId;
    this.schemaVersion = schemaVersion;
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
    ModelEntity modelEntity = ModelEntity.fromEntity(entity, schemaVersion);
    if (originalEntity == null) {
      try {
        List<Object> values =
            modelEntity.toMap(datasourceOperations.getDatabaseType()).values().stream().toList();
        queryAction.apply(
            connection,
            QueryGenerator.generateInsertQuery(
                ModelEntity.getAllColumnNames(schemaVersion),
                ModelEntity.TABLE_NAME,
                values,
                realmId));
      } catch (SQLException e) {
        if (datasourceOperations.isConstraintViolation(e)) {
          PolarisBaseEntity existingEntity =
              lookupEntityByName(
                  callCtx,
                  entity.getCatalogId(),
                  entity.getParentId(),
                  entity.getTypeCode(),
                  entity.getName());
          // This happens in two scenarios:
          // 1. PRIMARY KEY violated
          // 2. UNIQUE CONSTRAINT on (realm_id, catalog_id, parent_id, type_code, name) violated
          // With SERIALIZABLE isolation, the conflicting entity may _not_ be visible and
          // existingEntity can be null, which would cause an NPE in
          // EntityAlreadyExistsException.message().
          throw new EntityAlreadyExistsException(
              existingEntity != null ? existingEntity : entity, e);
        }
        throw new RuntimeException(
            String.format("Failed to write entity due to %s", e.getMessage()), e);
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
        List<Object> values =
            modelEntity.toMap(datasourceOperations.getDatabaseType()).values().stream().toList();
        int rowsUpdated =
            queryAction.apply(
                connection,
                QueryGenerator.generateUpdateQuery(
                    ModelEntity.getAllColumnNames(schemaVersion),
                    ModelEntity.TABLE_NAME,
                    values,
                    params));
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
    try {
      List<Object> values =
          modelGrantRecord.toMap(datasourceOperations.getDatabaseType()).values().stream().toList();
      datasourceOperations.executeUpdate(
          QueryGenerator.generateInsertQuery(
              ModelGrantRecord.ALL_COLUMNS, ModelGrantRecord.TABLE_NAME, values, realmId));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write to grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void writeEvents(@Nonnull List<PolarisEvent> events) {
    if (events.isEmpty()) {
      return; // or throw if empty list is invalid
    }

    try {
      // Generate the SQL using the first event as the reference
      PreparedQuery firstPreparedQuery =
          QueryGenerator.generateInsertQuery(
              ModelEvent.ALL_COLUMNS,
              ModelEvent.TABLE_NAME,
              ModelEvent.fromEvent(events.getFirst())
                  .toMap(datasourceOperations.getDatabaseType())
                  .values()
                  .stream()
                  .toList(),
              realmId);
      String expectedSql = firstPreparedQuery.sql();

      List<List<Object>> parametersList = new ArrayList<>();
      parametersList.add(firstPreparedQuery.parameters());

      // Process remaining events and verify SQL consistency
      for (int i = 1; i < events.size(); i++) {
        PolarisEvent event = events.get(i);
        PreparedQuery pq =
            QueryGenerator.generateInsertQuery(
                ModelEvent.ALL_COLUMNS,
                ModelEvent.TABLE_NAME,
                ModelEvent.fromEvent(event)
                    .toMap(datasourceOperations.getDatabaseType())
                    .values()
                    .stream()
                    .toList(),
                realmId);

        if (!expectedSql.equals(pq.sql())) {
          throw new RuntimeException("All events did not generate the same SQL");
        }

        parametersList.add(pq.parameters());
      }

      int totalUpdated =
          datasourceOperations.executeBatchUpdate(
              new QueryGenerator.PreparedBatchQuery(expectedSql, parametersList));

      if (totalUpdated == 0) {
        throw new SQLException("No events were inserted.");
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write events due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    ModelEntity modelEntity = ModelEntity.fromEntity(entity, schemaVersion);
    Map<String, Object> params =
        Map.of(
            "id",
            modelEntity.getId(),
            "catalog_id",
            modelEntity.getCatalogId(),
            "realm_id",
            realmId);
    try {
      datasourceOperations.executeUpdate(
          QueryGenerator.generateDeleteQuery(
              ModelEntity.getAllColumnNames(schemaVersion), ModelEntity.TABLE_NAME, params));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete entity due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteFromGrantRecords(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec);
    try {
      Map<String, Object> whereClause =
          modelGrantRecord.toMap(datasourceOperations.getDatabaseType());
      whereClause.put("realm_id", realmId);
      datasourceOperations.executeUpdate(
          QueryGenerator.generateDeleteQuery(
              ModelGrantRecord.ALL_COLUMNS, ModelGrantRecord.TABLE_NAME, whereClause));
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
          QueryGenerator.generateDeleteQueryForEntityGrantRecords(entity, realmId));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteAll(@Nonnull PolarisCallContext callCtx) {
    try {
      Map<String, Object> params = Map.of("realm_id", realmId);
      datasourceOperations.runWithinTransaction(
          connection -> {
            datasourceOperations.execute(
                connection,
                QueryGenerator.generateDeleteQuery(
                    ModelEntity.getAllColumnNames(schemaVersion), ModelEntity.TABLE_NAME, params));
            datasourceOperations.execute(
                connection,
                QueryGenerator.generateDeleteQuery(
                    ModelGrantRecord.ALL_COLUMNS, ModelGrantRecord.TABLE_NAME, params));
            datasourceOperations.execute(
                connection,
                QueryGenerator.generateDeleteQuery(
                    ModelPrincipalAuthenticationData.ALL_COLUMNS,
                    ModelPrincipalAuthenticationData.TABLE_NAME,
                    params));
            datasourceOperations.execute(
                connection,
                QueryGenerator.generateDeleteQuery(
                    ModelPolicyMappingRecord.ALL_COLUMNS,
                    ModelPolicyMappingRecord.TABLE_NAME,
                    params));
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
    return getPolarisBaseEntity(
        QueryGenerator.generateSelectQuery(
            ModelEntity.getAllColumnNames(schemaVersion), ModelEntity.TABLE_NAME, params));
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
    return getPolarisBaseEntity(
        QueryGenerator.generateSelectQuery(
            ModelEntity.getAllColumnNames(schemaVersion), ModelEntity.TABLE_NAME, params));
  }

  @Nullable
  private PolarisBaseEntity getPolarisBaseEntity(QueryGenerator.PreparedQuery query) {
    try {
      var results = datasourceOperations.executeSelect(query, new ModelEntity(schemaVersion));
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
    PreparedQuery query =
        QueryGenerator.generateSelectQueryWithEntityIds(realmId, schemaVersion, entityIds);
    try {
      Map<PolarisEntityId, PolarisBaseEntity> idMap =
          datasourceOperations.executeSelect(query, new ModelEntity(schemaVersion)).stream()
              .collect(
                  Collectors.toMap(
                      e -> new PolarisEntityId(e.getCatalogId(), e.getId()), Function.identity()));
      return entityIds.stream().map(idMap::get).collect(Collectors.toList());
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
            .filter(Objects::nonNull)
            .collect(
                Collectors.toMap(
                    entry -> new PolarisEntityId(entry.getCatalogId(), entry.getId()),
                    entry -> ModelEntity.fromEntity(entry, schemaVersion)));
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

  private PreparedQuery buildEntityQuery(
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      PolarisEntitySubType entitySubType,
      PageToken pageToken,
      List<String> queryProjections) {
    Map<String, Object> whereEquals =
        Map.of(
            "catalog_id",
            catalogId,
            "parent_id",
            parentId,
            "type_code",
            entityType.getCode(),
            "realm_id",
            realmId);

    if (entitySubType != PolarisEntitySubType.ANY_SUBTYPE) {
      Map<String, Object> updatedWhereEquals = new HashMap<>(whereEquals);
      updatedWhereEquals.put("sub_type_code", entitySubType.getCode());
      whereEquals = updatedWhereEquals;
    }

    String orderByColumnName = null;
    Map<String, Object> whereGreater;
    if (pageToken.paginationRequested()) {
      orderByColumnName = ModelEntity.ID_COLUMN;
      whereGreater =
          pageToken
              .valueAs(EntityIdToken.class)
              .map(
                  entityIdToken ->
                      Map.<String, Object>of(ModelEntity.ID_COLUMN, entityIdToken.entityId()))
              .orElse(Map.of());
    } else {
      whereGreater = Map.of();
    }

    return QueryGenerator.generateSelectQuery(
        queryProjections, ModelEntity.TABLE_NAME, whereEquals, whereGreater, orderByColumnName);
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
    try {
      PreparedQuery query =
          buildEntityQuery(
              catalogId,
              parentId,
              entityType,
              entitySubType,
              pageToken,
              ModelEntity.ENTITY_LOOKUP_COLUMNS);
      AtomicReference<Page<EntityNameLookupRecord>> results = new AtomicReference<>();
      datasourceOperations.executeSelectOverStream(
          query,
          new EntityNameLookupRecordConverter(),
          stream -> {
            results.set(
                Page.mapped(pageToken, stream, Function.identity(), EntityIdToken::fromEntity));
          });
      return results.get();
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve polaris entities due to %s", e.getMessage()), e);
    }
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
      @Nonnull PageToken pageToken) {
    try {
      PreparedQuery query =
          buildEntityQuery(
              catalogId,
              parentId,
              entityType,
              entitySubType,
              pageToken,
              ModelEntity.getAllColumnNames(schemaVersion));
      AtomicReference<Page<T>> results = new AtomicReference<>();
      datasourceOperations.executeSelectOverStream(
          query,
          new ModelEntity(schemaVersion),
          stream -> {
            var data = stream.filter(entityFilter);
            results.set(Page.mapped(pageToken, data, transformer, EntityIdToken::fromEntity));
          });
      return results.get();
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
    PolarisBaseEntity b =
        getPolarisBaseEntity(
            QueryGenerator.generateSelectQuery(
                ModelEntity.getAllColumnNames(schemaVersion), ModelEntity.TABLE_NAME, params));
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
    try {
      var results =
          datasourceOperations.executeSelect(
              QueryGenerator.generateSelectQuery(
                  ModelGrantRecord.ALL_COLUMNS, ModelGrantRecord.TABLE_NAME, params),
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
    Map<String, Object> params =
        Map.of(
            "securable_catalog_id",
            securableCatalogId,
            "securable_id",
            securableId,
            "realm_id",
            realmId);
    try {
      var results =
          datasourceOperations.executeSelect(
              QueryGenerator.generateSelectQuery(
                  ModelGrantRecord.ALL_COLUMNS, ModelGrantRecord.TABLE_NAME, params),
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
    Map<String, Object> params =
        Map.of(
            "grantee_catalog_id", granteeCatalogId, "grantee_id", granteeId, "realm_id", realmId);
    try {
      var results =
          datasourceOperations.executeSelect(
              QueryGenerator.generateSelectQuery(
                  ModelGrantRecord.ALL_COLUMNS, ModelGrantRecord.TABLE_NAME, params),
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
    Map<String, Object> params = new HashMap<>();
    params.put("realm_id", realmId);
    params.put("catalog_id", catalogId);
    params.put("parent_id", parentId);
    if (optionalEntityType != null) {
      params.put("type_code", optionalEntityType.getCode());
    }
    try {
      var results =
          datasourceOperations.executeSelect(
              QueryGenerator.generateSelectQuery(
                  ModelEntity.getAllColumnNames(schemaVersion), ModelEntity.TABLE_NAME, params),
              new ModelEntity(schemaVersion));
      return results != null && !results.isEmpty();
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve entities for catalogId: %s due to %s", catalogId, e.getMessage()),
          e);
    }
  }

  static int loadSchemaVersion(
      DatasourceOperations datasourceOperations, boolean fallbackOnDoesNotExist) {
    PreparedQuery query = QueryGenerator.generateVersionQuery();
    try {
      List<SchemaVersion> schemaVersion =
          datasourceOperations.executeSelect(query, new SchemaVersion());
      if (schemaVersion == null || schemaVersion.size() != 1) {
        throw new RuntimeException("Failed to retrieve schema version");
      }
      return schemaVersion.getFirst().getValue();
    } catch (SQLException e) {
      if (fallbackOnDoesNotExist && datasourceOperations.isRelationDoesNotExist(e)) {
        return SchemaVersion.MINIMUM.getValue();
      }
      LOGGER.error("Failed to load schema version due to {}", e.getMessage(), e);
      throw new IllegalStateException("Failed to retrieve schema version", e);
    }
  }

  static boolean entityTableExists(DatasourceOperations datasourceOperations) {
    PreparedQuery query = QueryGenerator.generateEntityTableExistQuery();
    try {
      List<PolarisBaseEntity> entities =
          datasourceOperations.executeSelect(query, new ModelEntity());
      return entities != null && !entities.isEmpty();
    } catch (SQLException e) {
      if (datasourceOperations.isRelationDoesNotExist(e)) {
        return false;
      }
      throw new IllegalStateException("Failed to check if Entities table exists", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @Nonnull PolarisCallContext callContext, T entity) {
    if (this.schemaVersion < 2) {
      return Optional.empty();
    }
    if (entity.getBaseLocation().chars().filter(ch -> ch == '/').count()
        > MAX_LOCATION_COMPONENTS) {
      return Optional.empty();
    }

    PreparedQuery query =
        QueryGenerator.generateOverlapQuery(
            realmId, schemaVersion, entity.getCatalogId(), entity.getBaseLocation());
    try {
      var results = datasourceOperations.executeSelect(query, new ModelEntity(schemaVersion));
      if (!results.isEmpty()) {
        StorageLocation entityLocation = StorageLocation.of(entity.getBaseLocation());
        for (PolarisBaseEntity result : results) {
          StorageLocation potentialSiblingLocation =
              StorageLocation.of(((LocationBasedEntity) result).getBaseLocation());
          if (entityLocation.isChildOf(potentialSiblingLocation)
              || potentialSiblingLocation.isChildOf(entityLocation)) {
            return Optional.of(Optional.of(potentialSiblingLocation.toString()));
          }
        }
      }
      return Optional.of(Optional.empty());
    } catch (SQLException e) {
      LOGGER.error(
          "Failed to retrieve location overlap for location {} due to {}",
          entity.getBaseLocation(),
          e.getMessage(),
          e);
      throw new RuntimeException(
          String.format(
              "Failed to retrieve location overlap for location: %s", entity.getBaseLocation()),
          e);
    }
  }

  @Nullable
  @Override
  public PolarisPrincipalSecrets loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    Map<String, Object> params = Map.of("principal_client_id", clientId, "realm_id", realmId);
    try {
      var results =
          datasourceOperations.executeSelect(
              QueryGenerator.generateSelectQuery(
                  ModelPrincipalAuthenticationData.ALL_COLUMNS,
                  ModelPrincipalAuthenticationData.TABLE_NAME,
                  params),
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
              loadPrincipalSecrets(callCtx, principalSecrets.getPrincipalClientId()));
    } while (lookupPrincipalSecrets != null);

    lookupPrincipalSecrets =
        ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(principalSecrets);

    // write new principal secrets
    try {
      List<Object> values =
          lookupPrincipalSecrets.toMap(datasourceOperations.getDatabaseType()).values().stream()
              .toList();
      datasourceOperations.executeUpdate(
          QueryGenerator.generateInsertQuery(
              ModelPrincipalAuthenticationData.ALL_COLUMNS,
              ModelPrincipalAuthenticationData.TABLE_NAME,
              values,
              realmId));
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
  public PolarisPrincipalSecrets storePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      long principalId,
      @Nonnull String resolvedClientId,
      String customClientSecret) {
    PolarisPrincipalSecrets principalSecrets =
        new PolarisPrincipalSecrets(principalId, resolvedClientId, customClientSecret);
    try {
      ModelPrincipalAuthenticationData modelPrincipalAuthenticationData =
          ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(principalSecrets);
      datasourceOperations.executeUpdate(
          QueryGenerator.generateInsertQuery(
              ModelPrincipalAuthenticationData.ALL_COLUMNS,
              ModelPrincipalAuthenticationData.TABLE_NAME,
              modelPrincipalAuthenticationData
                  .toMap(datasourceOperations.getDatabaseType())
                  .values()
                  .stream()
                  .toList(),
              realmId));
    } catch (SQLException e) {
      LOGGER.error(
          "Failed to reset PrincipalSecrets  for clientId: {}, due to {}",
          resolvedClientId,
          e.getMessage(),
          e);
      throw new RuntimeException(
          String.format("Failed to reset PrincipalSecrets for clientId: %s", resolvedClientId), e);
    }

    // return those
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
    diagnostics.checkNotNull(
        principalSecrets,
        "cannot_find_secrets",
        "client_id={} principalId={}",
        clientId,
        principalId);

    // ensure principal id is matching
    diagnostics.check(
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
    try {
      ModelPrincipalAuthenticationData modelPrincipalAuthenticationData =
          ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(principalSecrets);
      datasourceOperations.executeUpdate(
          QueryGenerator.generateUpdateQuery(
              ModelPrincipalAuthenticationData.ALL_COLUMNS,
              ModelPrincipalAuthenticationData.TABLE_NAME,
              modelPrincipalAuthenticationData
                  .toMap(datasourceOperations.getDatabaseType())
                  .values()
                  .stream()
                  .toList(),
              params));
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
    try {
      datasourceOperations.executeUpdate(
          QueryGenerator.generateDeleteQuery(
              ModelPrincipalAuthenticationData.ALL_COLUMNS,
              ModelPrincipalAuthenticationData.TABLE_NAME,
              params));
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
                ModelPolicyMappingRecord.fromPolicyMappingRecord(record);
            List<Object> values =
                modelPolicyMappingRecord
                    .toMap(datasourceOperations.getDatabaseType())
                    .values()
                    .stream()
                    .toList();
            PreparedQuery insertPolicyMappingQuery =
                QueryGenerator.generateInsertQuery(
                    ModelPolicyMappingRecord.ALL_COLUMNS,
                    ModelPolicyMappingRecord.TABLE_NAME,
                    values,
                    realmId);
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
      ModelPolicyMappingRecord modelPolicyMappingRecord =
          ModelPolicyMappingRecord.fromPolicyMappingRecord(record);
      PreparedQuery updateQuery =
          QueryGenerator.generateUpdateQuery(
              ModelPolicyMappingRecord.ALL_COLUMNS,
              ModelPolicyMappingRecord.TABLE_NAME,
              modelPolicyMappingRecord
                  .toMap(datasourceOperations.getDatabaseType())
                  .values()
                  .stream()
                  .toList(),
              updateClause);
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
    var modelPolicyMappingRecord = ModelPolicyMappingRecord.fromPolicyMappingRecord(record);
    try {
      Map<String, Object> objectMap =
          modelPolicyMappingRecord.toMap(datasourceOperations.getDatabaseType());
      objectMap.put("realm_id", realmId);
      datasourceOperations.executeUpdate(
          QueryGenerator.generateDeleteQuery(
              ModelPolicyMappingRecord.ALL_COLUMNS,
              ModelPolicyMappingRecord.TABLE_NAME,
              objectMap));
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
      Map<String, Object> queryParams = new LinkedHashMap<>();
      if (entity.getType() == PolarisEntityType.POLICY) {
        PolicyEntity policyEntity = PolicyEntity.of(entity);
        queryParams.put("policy_type_code", policyEntity.getPolicyTypeCode());
        queryParams.put("policy_catalog_id", policyEntity.getCatalogId());
        queryParams.put("policy_id", policyEntity.getId());
      } else {
        queryParams.put("target_catalog_id", entity.getCatalogId());
        queryParams.put("target_id", entity.getId());
      }
      queryParams.put("realm_id", realmId);
      datasourceOperations.executeUpdate(
          QueryGenerator.generateDeleteQuery(
              ModelPolicyMappingRecord.ALL_COLUMNS,
              ModelPolicyMappingRecord.TABLE_NAME,
              queryParams));
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
    List<PolarisPolicyMappingRecord> results =
        fetchPolicyMappingRecords(
            QueryGenerator.generateSelectQuery(
                ModelPolicyMappingRecord.ALL_COLUMNS, ModelPolicyMappingRecord.TABLE_NAME, params));
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
    return fetchPolicyMappingRecords(
        QueryGenerator.generateSelectQuery(
            ModelPolicyMappingRecord.ALL_COLUMNS, ModelPolicyMappingRecord.TABLE_NAME, params));
  }

  @Nonnull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @Nonnull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    Map<String, Object> params =
        Map.of("target_catalog_id", targetCatalogId, "target_id", targetId, "realm_id", realmId);
    return fetchPolicyMappingRecords(
        QueryGenerator.generateSelectQuery(
            ModelPolicyMappingRecord.ALL_COLUMNS, ModelPolicyMappingRecord.TABLE_NAME, params));
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
    return fetchPolicyMappingRecords(
        QueryGenerator.generateSelectQuery(
            ModelPolicyMappingRecord.ALL_COLUMNS, ModelPolicyMappingRecord.TABLE_NAME, params));
  }

  private List<PolarisPolicyMappingRecord> fetchPolicyMappingRecords(
      QueryGenerator.PreparedQuery query) {
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
        BaseMetaStoreManager.extractStorageConfiguration(diagnostics, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  @FunctionalInterface
  private interface QueryAction {
    Integer apply(Connection connection, QueryGenerator.PreparedQuery query) throws SQLException;
  }

  // ============================================================================
  // MetricsPersistence Implementation
  // ============================================================================

  /** Returns the datasource operations to use for metrics persistence. */
  private DatasourceOperations getMetricsDatasource() {
    return datasourceOperations;
  }

  @Override
  public void writeScanReport(@Nonnull ScanMetricsRecord record) {
    ModelScanMetricsReport model = ModelScanMetricsReport.fromRecord(record, realmId);
    writeScanMetricsReport(model);
  }

  @Override
  public void writeCommitReport(@Nonnull CommitMetricsRecord record) {
    ModelCommitMetricsReport model = ModelCommitMetricsReport.fromRecord(record, realmId);
    writeCommitMetricsReport(model);
  }

  // ========== Internal Metrics JDBC methods ==========

  private void writeScanMetricsReport(@Nonnull ModelScanMetricsReport report) {
    DatasourceOperations metricsOps = getMetricsDatasource();
    try {
      PreparedQuery pq =
          QueryGenerator.generateInsertQuery(
              ModelScanMetricsReport.ALL_COLUMNS,
              ModelScanMetricsReport.TABLE_NAME,
              report.toMap(metricsOps.getDatabaseType()).values().stream().toList(),
              realmId);
      metricsOps.executeUpdate(pq);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write scan metrics report due to %s", e.getMessage()), e);
    }
  }

  private void writeCommitMetricsReport(@Nonnull ModelCommitMetricsReport report) {
    DatasourceOperations metricsOps = getMetricsDatasource();
    try {
      PreparedQuery pq =
          QueryGenerator.generateInsertQuery(
              ModelCommitMetricsReport.ALL_COLUMNS,
              ModelCommitMetricsReport.TABLE_NAME,
              report.toMap(metricsOps.getDatabaseType()).values().stream().toList(),
              realmId);
      metricsOps.executeUpdate(pq);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to write commit metrics report due to %s", e.getMessage()), e);
    }
  }

  // ============================================================================
  // IdempotencyPersistence Implementation
  // ============================================================================

  @Override
  public ReserveResult reserve(
      String realmId,
      String idempotencyKey,
      String operationType,
      String normalizedResourceId,
      String principalHash,
      Instant expiresAt,
      String executorId,
      Instant now) {
    try {
      Map<String, Object> insertMap = new LinkedHashMap<>();
      insertMap.put(ModelIdempotencyRecord.IDEMPOTENCY_KEY, idempotencyKey);
      insertMap.put(ModelIdempotencyRecord.OPERATION_TYPE, operationType);
      insertMap.put(ModelIdempotencyRecord.RESOURCE_ID, normalizedResourceId);
      insertMap.put(ModelIdempotencyRecord.PRINCIPAL_HASH, principalHash);
      insertMap.put(ModelIdempotencyRecord.HTTP_STATUS, null);
      insertMap.put(ModelIdempotencyRecord.ERROR_SUBTYPE, null);
      insertMap.put(ModelIdempotencyRecord.RESPONSE_SUMMARY, null);
      insertMap.put(ModelIdempotencyRecord.FINALIZED_AT, null);
      insertMap.put(ModelIdempotencyRecord.CREATED_AT, Timestamp.from(now));
      insertMap.put(ModelIdempotencyRecord.UPDATED_AT, Timestamp.from(now));
      insertMap.put(ModelIdempotencyRecord.HEARTBEAT_AT, Timestamp.from(now));
      insertMap.put(ModelIdempotencyRecord.EXECUTOR_ID, executorId);
      insertMap.put(ModelIdempotencyRecord.EXPIRES_AT, Timestamp.from(expiresAt));

      List<Object> values = insertMap.values().stream().toList();
      PreparedQuery insert =
          QueryGenerator.generateInsertQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              values,
              realmId);
      datasourceOperations.executeUpdate(insert);
      return new ReserveResult(ReserveResultType.OWNED, Optional.empty());
    } catch (SQLException e) {
      if (datasourceOperations.isConstraintViolation(e)) {
        return new ReserveResult(
            ReserveResultType.DUPLICATE, loadIdempotencyRecord(realmId, idempotencyKey));
      }
      throw new IdempotencyPersistenceException("Failed to reserve idempotency key", e);
    }
  }

  @Override
  public Optional<IdempotencyRecord> loadIdempotencyRecord(String realmId, String idempotencyKey) {
    try {
      PreparedQuery query =
          QueryGenerator.generateSelectQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              Map.of(
                  ModelIdempotencyRecord.REALM_ID,
                  realmId,
                  ModelIdempotencyRecord.IDEMPOTENCY_KEY,
                  idempotencyKey));
      List<IdempotencyRecord> results =
          datasourceOperations.executeSelect(
              query,
              new Converter<>() {
                @Override
                public IdempotencyRecord fromResultSet(ResultSet rs) throws SQLException {
                  return ModelIdempotencyRecord.fromRow(realmId, rs);
                }

                @Override
                public Map<String, Object> toMap(DatabaseType databaseType) {
                  throw new UnsupportedOperationException("Not used for SELECT conversion");
                }
              });
      if (results.isEmpty()) {
        return Optional.empty();
      }
      if (results.size() > 1) {
        throw new IllegalStateException(
            "More than one idempotency record found for realm/key: "
                + realmId
                + "/"
                + idempotencyKey);
      }
      return Optional.of(results.getFirst());
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to load idempotency record", e);
    }
  }

  @Override
  public HeartbeatResult updateHeartbeat(
      String realmId, String idempotencyKey, String executorId, Instant now) {
    Optional<IdempotencyRecord> existing = loadIdempotencyRecord(realmId, idempotencyKey);
    if (existing.isEmpty()) {
      return HeartbeatResult.NOT_FOUND;
    }

    IdempotencyRecord record = existing.get();
    if (record.httpStatus() != null) {
      return HeartbeatResult.FINALIZED;
    }
    if (record.executorId() == null || !record.executorId().equals(executorId)) {
      return HeartbeatResult.LOST_OWNERSHIP;
    }

    PreparedQuery update =
        QueryGenerator.generateUpdateQuery(
            ModelIdempotencyRecord.ALL_COLUMNS,
            ModelIdempotencyRecord.TABLE_NAME,
            Map.of(
                ModelIdempotencyRecord.HEARTBEAT_AT,
                Timestamp.from(now),
                ModelIdempotencyRecord.UPDATED_AT,
                Timestamp.from(now)),
            Map.of(
                ModelIdempotencyRecord.REALM_ID,
                realmId,
                ModelIdempotencyRecord.IDEMPOTENCY_KEY,
                idempotencyKey,
                ModelIdempotencyRecord.EXECUTOR_ID,
                executorId),
            Map.of(),
            Map.of(),
            Set.of(ModelIdempotencyRecord.HTTP_STATUS),
            Set.of());

    try {
      int updated = datasourceOperations.executeUpdate(update);
      if (updated > 0) {
        return HeartbeatResult.UPDATED;
      }
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to update idempotency heartbeat", e);
    }

    Optional<IdempotencyRecord> after = loadIdempotencyRecord(realmId, idempotencyKey);
    if (after.isEmpty()) {
      return HeartbeatResult.NOT_FOUND;
    }
    if (after.get().httpStatus() != null) {
      return HeartbeatResult.FINALIZED;
    }
    return HeartbeatResult.LOST_OWNERSHIP;
  }

  @Override
  public boolean cancelInProgressReservation(
      String realmId, String idempotencyKey, String executorId) {
    try {
      PreparedQuery delete =
          QueryGenerator.generateDeleteQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              Map.of(
                  ModelIdempotencyRecord.REALM_ID,
                  realmId,
                  ModelIdempotencyRecord.IDEMPOTENCY_KEY,
                  idempotencyKey,
                  ModelIdempotencyRecord.EXECUTOR_ID,
                  executorId),
              Map.of(),
              Map.of(),
              Set.of(ModelIdempotencyRecord.HTTP_STATUS),
              Set.of());
      return datasourceOperations.executeUpdate(delete) > 0;
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to cancel idempotency reservation", e);
    }
  }

  @Override
  public boolean finalizeRecord(
      String realmId,
      String idempotencyKey,
      String executorId,
      Integer httpStatus,
      String errorSubtype,
      String responseSummary,
      Instant finalizedAt) {
    Map<String, Object> setClause = new LinkedHashMap<>();
    setClause.put(ModelIdempotencyRecord.HTTP_STATUS, httpStatus);
    setClause.put(ModelIdempotencyRecord.ERROR_SUBTYPE, errorSubtype);
    setClause.put(ModelIdempotencyRecord.RESPONSE_SUMMARY, responseSummary);
    setClause.put(ModelIdempotencyRecord.FINALIZED_AT, Timestamp.from(finalizedAt));
    setClause.put(ModelIdempotencyRecord.UPDATED_AT, Timestamp.from(finalizedAt));

    Map<String, Object> whereEquals = new HashMap<>();
    whereEquals.put(ModelIdempotencyRecord.REALM_ID, realmId);
    whereEquals.put(ModelIdempotencyRecord.IDEMPOTENCY_KEY, idempotencyKey);
    whereEquals.put(ModelIdempotencyRecord.EXECUTOR_ID, executorId);

    PreparedQuery update =
        QueryGenerator.generateUpdateQuery(
            ModelIdempotencyRecord.ALL_COLUMNS,
            ModelIdempotencyRecord.TABLE_NAME,
            setClause,
            whereEquals,
            Map.of(),
            Map.of(),
            Set.of(ModelIdempotencyRecord.HTTP_STATUS),
            Set.of());

    try {
      return datasourceOperations.executeUpdate(update) > 0;
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to finalize idempotency record", e);
    }
  }

  @Override
  public int purgeExpired(String realmId, Instant before) {
    try {
      PreparedQuery delete =
          QueryGenerator.generateDeleteQuery(
              ModelIdempotencyRecord.ALL_COLUMNS,
              ModelIdempotencyRecord.TABLE_NAME,
              Map.of(ModelIdempotencyRecord.REALM_ID, realmId),
              Map.of(),
              Map.of(ModelIdempotencyRecord.EXPIRES_AT, Timestamp.from(before)),
              Set.of(),
              Set.of(ModelIdempotencyRecord.EXPIRES_AT));
      return datasourceOperations.executeUpdate(delete);
    } catch (SQLException e) {
      throw new IdempotencyPersistenceException("Failed to purge expired idempotency records", e);
    }
  }
}
