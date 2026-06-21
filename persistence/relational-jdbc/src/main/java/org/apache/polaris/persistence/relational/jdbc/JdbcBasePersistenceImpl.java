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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEntityUtils;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.lineage.LineageColumnEdge;
import org.apache.polaris.core.lineage.LineageData;
import org.apache.polaris.core.lineage.LineageDataset;
import org.apache.polaris.core.lineage.LineageDirection;
import org.apache.polaris.core.lineage.LineageEdge;
import org.apache.polaris.core.lineage.LineageFieldMapping;
import org.apache.polaris.core.lineage.LineageGranularity;
import org.apache.polaris.core.lineage.LineageGraph;
import org.apache.polaris.core.lineage.LineageNode;
import org.apache.polaris.core.lineage.LineageNodeType;
import org.apache.polaris.core.lineage.LineagePersistence;
import org.apache.polaris.core.lineage.LineageQueryRequest;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.metrics.CommitMetricsRecord;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
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
import org.apache.polaris.persistence.relational.jdbc.models.ModelLineageColumnEdge;
import org.apache.polaris.persistence.relational.jdbc.models.ModelLineageDataset;
import org.apache.polaris.persistence.relational.jdbc.models.ModelLineageEdge;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPolicyMappingRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.SchemaVersion;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcBasePersistenceImpl
    implements BasePersistence, IntegrationPersistence, MetricsPersistence, LineagePersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBasePersistenceImpl.class);

  private final PolarisDiagnostics diagnostics;
  private final DatasourceOperations datasourceOperations;
  private final PrincipalSecretsGenerator secretsGenerator;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final String realmId;
  private final int schemaVersion;

  // The max number of components a location can have before the optimized sibling check is not used
  private static final int MAX_LOCATION_COMPONENTS = 40;
  private static final int MIN_LINEAGE_SCHEMA_VERSION = 5;

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
  public long generateNewId(@NonNull PolarisCallContext callCtx) {
    return IdGenerator.getIdGenerator().nextId();
  }

  @Override
  public void writeEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
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
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisBaseEntity> entities,
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
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
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
        if (datasourceOperations.isUniquenessConstraintViolation(e)) {
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
      // CAS on both entity_version and grant_records_version because grant operations only
      // bump grant_records_version without touching entity_version.
      Map<String, Object> params =
          Map.of(
              "id",
              originalEntity.getId(),
              "catalog_id",
              originalEntity.getCatalogId(),
              "entity_version",
              originalEntity.getEntityVersion(),
              "grant_records_version",
              originalEntity.getGrantRecordsVersion(),
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
              "Entity '%s' id '%s' concurrently modified; expected entity_version=%s, grant_records_version=%s",
              originalEntity.getName(),
              originalEntity.getId(),
              originalEntity.getEntityVersion(),
              originalEntity.getGrantRecordsVersion());
        }
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format("Failed to write entity due to %s", e.getMessage()), e);
      }
    }
  }

  @Override
  public void writeToGrantRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
    ModelGrantRecord modelGrantRecord = ModelGrantRecord.fromGrantRecord(grantRec);
    try {
      List<Object> values =
          modelGrantRecord.toMap(datasourceOperations.getDatabaseType()).values().stream().toList();
      datasourceOperations.executeUpdate(
          QueryGenerator.generateInsertQuery(
              ModelGrantRecord.ALL_COLUMNS, ModelGrantRecord.TABLE_NAME, values, realmId));
    } catch (SQLException e) {
      if (datasourceOperations.isUniquenessConstraintViolation(e)) {
        LOGGER.debug("Grant record already exists; treating as no-op: {}", grantRec);
        return;
      }
      throw new RuntimeException(
          String.format("Failed to write to grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void writeEvents(@NonNull List<EventEntity> events) {
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
        EventEntity event = events.get(i);
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
  public void deleteEntity(@NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity entity) {
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
      @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
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
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore entity,
      @NonNull List<PolarisGrantRecord> grantsOnGrantee,
      @NonNull List<PolarisGrantRecord> grantsOnSecurable) {
    try {
      datasourceOperations.executeUpdate(
          QueryGenerator.generateDeleteQueryForEntityGrantRecords(entity, realmId));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete grant records due to %s", e.getMessage()), e);
    }
  }

  @Override
  public void deleteAll(@NonNull PolarisCallContext callCtx) {
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
            if (schemaVersion >= MIN_LINEAGE_SCHEMA_VERSION) {
              datasourceOperations.execute(
                  connection,
                  QueryGenerator.generateDeleteQuery(
                      ModelLineageColumnEdge.ALL_COLUMNS,
                      ModelLineageColumnEdge.TABLE_NAME,
                      params));
              datasourceOperations.execute(
                  connection,
                  QueryGenerator.generateDeleteQuery(
                      ModelLineageEdge.ALL_COLUMNS, ModelLineageEdge.TABLE_NAME, params));
              datasourceOperations.execute(
                  connection,
                  QueryGenerator.generateDeleteQuery(
                      ModelLineageDataset.ALL_COLUMNS, ModelLineageDataset.TABLE_NAME, params));
            }
            return true;
          });
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to delete all due to %s", e.getMessage()), e);
    }
  }

  @Override
  public PolarisBaseEntity lookupEntity(
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    Map<String, Object> params =
        Map.of("catalog_id", catalogId, "id", entityId, "type_code", typeCode, "realm_id", realmId);
    return getPolarisBaseEntity(
        QueryGenerator.generateSelectQuery(
            ModelEntity.getAllColumnNames(schemaVersion), ModelEntity.TABLE_NAME, params));
  }

  @Override
  public PolarisBaseEntity lookupEntityByName(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @NonNull String name) {
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

  @NonNull
  @Override
  public List<PolarisBaseEntity> lookupEntities(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
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

  @NonNull
  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
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

  @NonNull
  @Override
  public Page<EntityNameLookupRecord> listEntities(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
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
      @NonNull PageToken pageToken) {
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
      @NonNull PolarisCallContext callCtx, long catalogId, long entityId) {

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
      @NonNull PolarisCallContext callCtx,
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

  @NonNull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      @NonNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
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

  @NonNull
  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      @NonNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
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
      @NonNull PolarisCallContext callContext,
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
          @NonNull PolarisCallContext callContext, T entity) {
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
          // JDBC materializes persisted rows as PolarisBaseEntity. Resolve the sibling location
          // via PolarisEntityUtils instead of casting to LocationBasedEntity.
          Optional<String> overlappingSiblingLocation =
              PolarisEntityUtils.asLocationBasedEntity(PolarisEntity.of(result))
                  .map(LocationBasedEntity::getBaseLocation)
                  .filter(location -> location != null && !location.isBlank())
                  .map(StorageLocation::of)
                  .filter(
                      potentialSiblingLocation ->
                          entityLocation.isChildOf(potentialSiblingLocation)
                              || potentialSiblingLocation.isChildOf(entityLocation))
                  .map(StorageLocation::toString);
          if (overlappingSiblingLocation.isPresent()) {
            return Optional.of(overlappingSiblingLocation);
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
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
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

  @NonNull
  @Override
  public PolarisPrincipalSecrets generateNewPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String principalName, long principalId) {
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
      @NonNull PolarisCallContext callCtx,
      long principalId,
      @NonNull String resolvedClientId,
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
      if (datasourceOperations.isUniquenessConstraintViolation(e)) {
        throw new AlreadyExistsException(e.getMessage(), e);
      }
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
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash) {
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
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
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
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
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
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisPolicyMappingRecord record,
      @NonNull PreparedQuery insertQuery,
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
      datasourceOperations.execute(connection, insertQuery);
    }
    return true;
  }

  @Override
  public void deleteFromPolicyMappingRecords(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisPolicyMappingRecord record) {
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
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity entity,
      @NonNull List<PolarisPolicyMappingRecord> mappingOnTarget,
      @NonNull List<PolarisPolicyMappingRecord> mappingOnPolicy) {
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
      @NonNull PolarisCallContext callCtx,
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

  @NonNull
  @Override
  public List<PolarisPolicyMappingRecord> loadPoliciesOnTargetByType(
      @NonNull PolarisCallContext callCtx,
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

  @NonNull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllPoliciesOnTarget(
      @NonNull PolarisCallContext callCtx, long targetCatalogId, long targetId) {
    Map<String, Object> params =
        Map.of("target_catalog_id", targetCatalogId, "target_id", targetId, "realm_id", realmId);
    return fetchPolicyMappingRecords(
        QueryGenerator.generateSelectQuery(
            ModelPolicyMappingRecord.ALL_COLUMNS, ModelPolicyMappingRecord.TABLE_NAME, params));
  }

  @NonNull
  @Override
  public List<PolarisPolicyMappingRecord> loadAllTargetsOnPolicy(
      @NonNull PolarisCallContext callCtx,
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
  public PolarisStorageIntegration createStorageIntegration(
      @NonNull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    // No-op in OSS: the storage integration is resolved at credential-vending time via
    // PolarisStorageIntegrationProvider.getStorageIntegration(resolvedEntityPath). This hook
    // remains available for custom deployments that need to allocate/lease external state
    // atomically with the catalog-creation transaction.
    return null;
  }

  @Override
  public void persistStorageIntegrationIfNeeded(
      @NonNull PolarisCallContext callContext,
      @NonNull PolarisBaseEntity entity,
      @Nullable PolarisStorageIntegration storageIntegration) {}

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
  public void writeScanReport(@NonNull ScanMetricsRecord record) {
    ModelScanMetricsReport model = ModelScanMetricsReport.fromRecord(record, realmId);
    writeScanMetricsReport(model);
  }

  @Override
  public void writeCommitReport(@NonNull CommitMetricsRecord record) {
    ModelCommitMetricsReport model = ModelCommitMetricsReport.fromRecord(record, realmId);
    writeCommitMetricsReport(model);
  }

  // ========== Internal Metrics JDBC methods ==========

  private void writeScanMetricsReport(@NonNull ModelScanMetricsReport report) {
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

  private void writeCommitMetricsReport(@NonNull ModelCommitMetricsReport report) {
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
  // LineagePersistence Implementation
  // ============================================================================

  @Override
  public void upsertDatasets(RealmContext realmContext, List<LineageDataset> datasets) {
    verifyLineagePersistenceSupported();
    String realmId = realmContext.getRealmIdentifier();
    long nowMillis = Instant.now().toEpochMilli();
    for (LineageDataset dataset : datasets) {
      long datasetId =
          lookupLineageDataset(realmId, dataset)
              .map(ModelLineageDataset::getDatasetId)
              .orElseGet(IdGenerator.getIdGenerator()::nextId);
      ModelLineageDataset model =
          ModelLineageDataset.fromDataset(dataset, realmId, datasetId, nowMillis);
      try {
        datasourceOperations.executeUpdate(generateLineageDatasetUpsert(model));
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format("Failed to upsert lineage dataset due to %s", e.getMessage()), e);
      }
    }
  }

  @Override
  public void replaceDatasetEdges(
      RealmContext realmContext,
      List<LineageDataset> targetDatasets,
      List<LineageEdge> edges,
      Instant lastEventAt) {
    verifyLineagePersistenceSupported();
    String realmId = realmContext.getRealmIdentifier();
    long lastEventAtMillis = lastEventAt.toEpochMilli();
    Map<Long, List<ModelLineageEdge>> edgesByTargetDatasetId = new LinkedHashMap<>();
    for (LineageDataset targetDataset : targetDatasets) {
      edgesByTargetDatasetId.putIfAbsent(
          requireLineageDatasetId(realmId, targetDataset), new ArrayList<>());
    }
    for (LineageEdge edge : edges) {
      ModelLineageEdge model =
          ModelLineageEdge.fromIds(
              realmId,
              requireLineageDatasetId(realmId, edge.source()),
              requireLineageDatasetId(realmId, edge.target()),
              lastEventAtMillis);
      edgesByTargetDatasetId
          .computeIfAbsent(model.getTargetDatasetId(), ignored -> new ArrayList<>())
          .add(model);
    }

    try {
      datasourceOperations.runWithinTransaction(
          connection -> {
            for (Map.Entry<Long, List<ModelLineageEdge>> entry :
                edgesByTargetDatasetId.entrySet()) {
              long targetDatasetId = entry.getKey();
              Optional<Long> currentLastEventAt =
                  loadLineageDatasetLastEventAt(connection, realmId, targetDatasetId);
              if (currentLastEventAt.isPresent() && currentLastEventAt.get() > lastEventAtMillis) {
                continue;
              }

              List<Long> sourceDatasetIds =
                  entry.getValue().stream()
                      .map(ModelLineageEdge::getSourceDatasetId)
                      .distinct()
                      .toList();
              datasourceOperations.execute(
                  connection,
                  generateDeleteStaleLineageEdges(realmId, targetDatasetId, sourceDatasetIds));
              datasourceOperations.execute(
                  connection,
                  generateDeleteStaleLineageColumnEdges(
                      realmId, targetDatasetId, sourceDatasetIds));
              for (ModelLineageEdge model : entry.getValue()) {
                datasourceOperations.execute(connection, generateLineageEdgeUpsert(model));
              }
              datasourceOperations.execute(
                  connection,
                  generateLineageDatasetLastEventAtUpdate(
                      realmId, targetDatasetId, lastEventAtMillis));
            }
            return true;
          });
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to upsert lineage edge due to %s", e.getMessage()), e);
    }
  }

  private Optional<Long> loadLineageDatasetLastEventAt(
      Connection connection, String realmId, long targetDatasetId) throws SQLException {
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageDataset.TABLE_NAME);
    try (PreparedStatement statement =
        connection.prepareStatement(
            "SELECT last_lineage_event_at FROM "
                + table
                + " WHERE realm_id = ? AND dataset_id = ?")) {
      statement.setString(1, realmId);
      statement.setLong(2, targetDatasetId);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return Optional.empty();
        }
        long value = resultSet.getLong(1);
        return resultSet.wasNull() ? Optional.empty() : Optional.of(value);
      }
    }
  }

  @Override
  public void upsertColumnEdges(
      RealmContext realmContext, List<LineageColumnEdge> columnEdges, Instant lastEventAt) {
    verifyLineagePersistenceSupported();
    String realmId = realmContext.getRealmIdentifier();
    long lastEventAtMillis = lastEventAt.toEpochMilli();
    for (LineageColumnEdge columnEdge : columnEdges) {
      ModelLineageDataset targetDataset =
          requireLineageDataset(realmId, columnEdge.target().dataset());
      Long currentLastEventAt = targetDataset.getLastLineageEventAt();
      if (currentLastEventAt != null && currentLastEventAt > lastEventAtMillis) {
        continue;
      }

      ModelLineageColumnEdge model =
          ModelLineageColumnEdge.fromIds(
              realmId,
              requireLineageDatasetId(realmId, columnEdge.source().dataset()),
              columnEdge.source().field(),
              targetDataset.getDatasetId(),
              columnEdge.target().field(),
              lastEventAtMillis);
      try {
        datasourceOperations.executeUpdate(generateLineageColumnEdgeUpsert(model));
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format("Failed to upsert lineage column edge due to %s", e.getMessage()), e);
      }
    }
  }

  @Override
  public LineageGraph loadLineage(RealmContext realmContext, LineageQueryRequest request) {
    verifyLineagePersistenceSupported();
    String realmId = realmContext.getRealmIdentifier();
    Optional<ModelLineageDataset> requested =
        lookupLineageDatasetByNodeId(realmId, request.nodeId());
    if (requested.isEmpty()) {
      return new LineageGraph(
          new LineageNode(request.nodeId(), LineageNodeType.DATASET, null, true),
          List.of(),
          List.of());
    }

    ModelLineageDataset dataset = requested.get();
    boolean includeColumns = request.granularity() == LineageGranularity.COLUMN;
    List<LineageNode> upstream =
        request.direction() == LineageDirection.UPSTREAM
                || request.direction() == LineageDirection.BOTH
            ? loadAdjacentLineageNodes(realmId, dataset.getDatasetId(), true, includeColumns)
            : List.of();
    List<LineageNode> downstream =
        request.direction() == LineageDirection.DOWNSTREAM
                || request.direction() == LineageDirection.BOTH
            ? loadAdjacentLineageNodes(realmId, dataset.getDatasetId(), false, includeColumns)
            : List.of();

    return new LineageGraph(toLineageNode(dataset, List.of()), upstream, downstream);
  }

  private Optional<ModelLineageDataset> lookupLineageDataset(
      String realmId, LineageDataset dataset) {
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageDataset.TABLE_NAME);
    PreparedQuery query =
        new PreparedQuery(
            "SELECT realm_id, dataset_id, catalog, namespace, name, polaris_entity_id, last_lineage_event_at, created_at, updated_at "
                + "FROM "
                + table
                + " WHERE realm_id = ? AND namespace = ? AND name = ?",
            List.of(realmId, dataset.namespace(), dataset.name()));
    try {
      List<ModelLineageDataset> results =
          datasourceOperations.executeSelect(query, ModelLineageDataset.CONVERTER);
      return results.stream().findFirst();
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to load lineage dataset due to %s", e.getMessage()), e);
    }
  }

  private Optional<ModelLineageDataset> lookupLineageDatasetByNodeId(
      String realmId, String nodeId) {
    return parseLineageDatasetNodeId(nodeId)
        .flatMap(dataset -> lookupLineageDataset(realmId, dataset));
  }

  private Optional<LineageDataset> parseLineageDatasetNodeId(String nodeId) {
    String prefix = "dataset:";
    if (!nodeId.startsWith(prefix)) {
      return Optional.empty();
    }
    String identity = nodeId.substring(prefix.length());
    int catalogSeparator = identity.lastIndexOf(':');
    int nameSeparator = identity.lastIndexOf('.');
    if (catalogSeparator < 0 || nameSeparator <= catalogSeparator + 1) {
      return Optional.empty();
    }
    String catalog = identity.substring(0, catalogSeparator);
    String namespace = identity.substring(catalogSeparator + 1, nameSeparator);
    String name = identity.substring(nameSeparator + 1);
    return Optional.of(new LineageDataset(catalog, namespace, name));
  }

  private long requireLineageDatasetId(String realmId, LineageDataset dataset) {
    return requireLineageDataset(realmId, dataset).getDatasetId();
  }

  private ModelLineageDataset requireLineageDataset(String realmId, LineageDataset dataset) {
    return lookupLineageDataset(realmId, dataset)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Lineage dataset '%s.%s' does not exist in realm '%s'. Call upsertDatasets before writing edges.",
                        dataset.namespace(), dataset.name(), realmId)));
  }

  private List<LineageNode> loadAdjacentLineageNodes(
      String realmId, long datasetId, boolean upstream, boolean includeColumns) {
    String edgesTable = QueryGenerator.getFullyQualifiedTableName(ModelLineageEdge.TABLE_NAME);
    String datasetsTable =
        QueryGenerator.getFullyQualifiedTableName(ModelLineageDataset.TABLE_NAME);
    String adjacentEdgeColumn = upstream ? "source_dataset_id" : "target_dataset_id";
    String requestedEdgeColumn = upstream ? "target_dataset_id" : "source_dataset_id";
    PreparedQuery query =
        new PreparedQuery(
            "SELECT d.realm_id, d.dataset_id, d.catalog, d.namespace, d.name, d.polaris_entity_id, d.last_lineage_event_at, d.created_at, d.updated_at "
                + "FROM "
                + edgesTable
                + " e JOIN "
                + datasetsTable
                + " d ON d.realm_id = e.realm_id AND d.dataset_id = e."
                + adjacentEdgeColumn
                + " WHERE e.realm_id = ? AND e."
                + requestedEdgeColumn
                + " = ?",
            List.of(realmId, datasetId));
    try {
      List<ModelLineageDataset> datasets =
          datasourceOperations.executeSelect(query, ModelLineageDataset.CONVERTER);
      return datasets.stream()
          .map(
              dataset ->
                  toLineageNode(
                      dataset,
                      includeColumns
                          ? loadFieldMappings(realmId, datasetId, dataset.getDatasetId(), upstream)
                          : List.of()))
          .toList();
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to load adjacent lineage nodes due to %s", e.getMessage()), e);
    }
  }

  private List<LineageFieldMapping> loadFieldMappings(
      String realmId, long requestedDatasetId, long adjacentDatasetId, boolean upstream) {
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageColumnEdge.TABLE_NAME);
    long sourceDatasetId = upstream ? adjacentDatasetId : requestedDatasetId;
    long targetDatasetId = upstream ? requestedDatasetId : adjacentDatasetId;
    PreparedQuery query =
        new PreparedQuery(
            "SELECT source_field, target_field FROM "
                + table
                + " WHERE realm_id = ? AND source_dataset_id = ? AND target_dataset_id = ?",
            List.of(realmId, sourceDatasetId, targetDatasetId));
    try {
      return datasourceOperations.executeSelect(query, new LineageFieldMappingConverter());
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to load lineage field mappings due to %s", e.getMessage()), e);
    }
  }

  private static LineageNode toLineageNode(
      ModelLineageDataset dataset, List<LineageFieldMapping> fieldMappings) {
    LineageData data =
        new LineageData(
            OptionalLong.empty(),
            OptionalLong.of(dataset.getDatasetId()),
            dataset.getNamespace(),
            dataset.getName(),
            null,
            OptionalLong.of(dataset.getCreatedAt()),
            OptionalLong.of(dataset.getUpdatedAt()));
    return new LineageNode(
        lineageNodeId(dataset), LineageNodeType.DATASET, data, false, fieldMappings);
  }

  private static String lineageNodeId(ModelLineageDataset dataset) {
    return String.format(
        "dataset:%s:%s.%s", dataset.getCatalog(), dataset.getNamespace(), dataset.getName());
  }

  private void verifyLineagePersistenceSupported() {
    if (schemaVersion >= MIN_LINEAGE_SCHEMA_VERSION) {
      return;
    }
    throw new IllegalStateException(
        String.format(
            "Lineage persistence requires JDBC schema version %d or newer for realm '%s'; current schema version is %d. Upgrade the JDBC schema to v%d to persist lineage.",
            MIN_LINEAGE_SCHEMA_VERSION, realmId, schemaVersion, MIN_LINEAGE_SCHEMA_VERSION));
  }

  private PreparedQuery generateLineageDatasetUpsert(ModelLineageDataset model) {
    List<Object> values = new ArrayList<>();
    values.add(model.getRealmId());
    values.add(model.getDatasetId());
    values.add(model.getCatalog());
    values.add(model.getNamespace());
    values.add(model.getName());
    values.add(model.getPolarisEntityId());
    values.add(model.getLastLineageEventAt());
    values.add(model.getCreatedAt());
    values.add(model.getUpdatedAt());
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageDataset.TABLE_NAME);
    if (datasourceOperations.getDatabaseType().equals(DatabaseType.H2)) {
      return new PreparedQuery(
          "MERGE INTO "
              + table
              + " t USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) "
              + "v(realm_id, dataset_id, catalog, namespace, name, polaris_entity_id, last_lineage_event_at, created_at, updated_at) "
              + "ON t.realm_id = v.realm_id AND t.namespace = v.namespace AND t.name = v.name "
              + "WHEN MATCHED THEN UPDATE SET catalog = v.catalog, polaris_entity_id = v.polaris_entity_id, updated_at = v.updated_at "
              + "WHEN NOT MATCHED THEN INSERT (realm_id, dataset_id, catalog, namespace, name, polaris_entity_id, last_lineage_event_at, created_at, updated_at) "
              + "VALUES (v.realm_id, v.dataset_id, v.catalog, v.namespace, v.name, v.polaris_entity_id, v.last_lineage_event_at, v.created_at, v.updated_at)",
          values);
    }
    return new PreparedQuery(
        "INSERT INTO "
            + table
            + " (realm_id, dataset_id, catalog, namespace, name, polaris_entity_id, last_lineage_event_at, created_at, updated_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            + "ON CONFLICT (realm_id, namespace, name) DO UPDATE SET "
            + "catalog = EXCLUDED.catalog, "
            + "polaris_entity_id = EXCLUDED.polaris_entity_id, "
            + "updated_at = EXCLUDED.updated_at",
        values);
  }

  private PreparedQuery generateLineageEdgeUpsert(ModelLineageEdge model) {
    List<Object> values =
        List.of(
            model.getRealmId(),
            model.getSourceDatasetId(),
            model.getTargetDatasetId(),
            model.getLastEventAt());
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageEdge.TABLE_NAME);
    if (datasourceOperations.getDatabaseType().equals(DatabaseType.H2)) {
      return new PreparedQuery(
          "MERGE INTO "
              + table
              + " t USING (VALUES (?, ?, ?, ?)) "
              + "v(realm_id, source_dataset_id, target_dataset_id, last_event_at) "
              + "ON t.realm_id = v.realm_id "
              + "AND t.source_dataset_id = v.source_dataset_id "
              + "AND t.target_dataset_id = v.target_dataset_id "
              + "WHEN MATCHED THEN UPDATE SET last_event_at = GREATEST(t.last_event_at, v.last_event_at) "
              + "WHEN NOT MATCHED THEN INSERT (realm_id, source_dataset_id, target_dataset_id, last_event_at) "
              + "VALUES (v.realm_id, v.source_dataset_id, v.target_dataset_id, v.last_event_at)",
          values);
    }
    return new PreparedQuery(
        "INSERT INTO "
            + table
            + " (realm_id, source_dataset_id, target_dataset_id, last_event_at) "
            + "VALUES (?, ?, ?, ?) "
            + "ON CONFLICT (realm_id, source_dataset_id, target_dataset_id) DO UPDATE SET "
            + "last_event_at = GREATEST("
            + table
            + ".last_event_at, EXCLUDED.last_event_at)",
        values);
  }

  private PreparedQuery generateDeleteStaleLineageEdges(
      String realmId, long targetDatasetId, List<Long> sourceDatasetIds) {
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageEdge.TABLE_NAME);
    List<Object> values = new ArrayList<>();
    values.add(realmId);
    values.add(targetDatasetId);
    values.addAll(sourceDatasetIds);
    return new PreparedQuery(
        "DELETE FROM "
            + table
            + " WHERE realm_id = ? AND target_dataset_id = ?"
            + sourceExclusionPredicate(sourceDatasetIds),
        values);
  }

  private PreparedQuery generateDeleteStaleLineageColumnEdges(
      String realmId, long targetDatasetId, List<Long> sourceDatasetIds) {
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageColumnEdge.TABLE_NAME);
    List<Object> values = new ArrayList<>();
    values.add(realmId);
    values.add(targetDatasetId);
    values.addAll(sourceDatasetIds);
    return new PreparedQuery(
        "DELETE FROM "
            + table
            + " WHERE realm_id = ? AND target_dataset_id = ?"
            + sourceExclusionPredicate(sourceDatasetIds),
        values);
  }

  private PreparedQuery generateLineageDatasetLastEventAtUpdate(
      String realmId, long targetDatasetId, long lastEventAtMillis) {
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageDataset.TABLE_NAME);
    return new PreparedQuery(
        "UPDATE "
            + table
            + " SET last_lineage_event_at = CASE "
            + "WHEN last_lineage_event_at IS NULL OR last_lineage_event_at < ? THEN ? "
            + "ELSE last_lineage_event_at END, "
            + "updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END "
            + "WHERE realm_id = ? AND dataset_id = ?",
        List.of(
            lastEventAtMillis,
            lastEventAtMillis,
            lastEventAtMillis,
            lastEventAtMillis,
            realmId,
            targetDatasetId));
  }

  private static String sourceExclusionPredicate(List<Long> sourceDatasetIds) {
    if (sourceDatasetIds.isEmpty()) {
      return "";
    }
    return " AND source_dataset_id NOT IN (" + placeholders(sourceDatasetIds.size()) + ")";
  }

  private static String placeholders(int count) {
    return String.join(", ", Collections.nCopies(count, "?"));
  }

  private PreparedQuery generateLineageColumnEdgeUpsert(ModelLineageColumnEdge model) {
    List<Object> values =
        List.of(
            model.getRealmId(),
            model.getSourceDatasetId(),
            model.getSourceField(),
            model.getTargetDatasetId(),
            model.getTargetField(),
            model.getLastEventAt());
    String table = QueryGenerator.getFullyQualifiedTableName(ModelLineageColumnEdge.TABLE_NAME);
    if (datasourceOperations.getDatabaseType().equals(DatabaseType.H2)) {
      return new PreparedQuery(
          "MERGE INTO "
              + table
              + " t USING (VALUES (?, ?, ?, ?, ?, ?)) "
              + "v(realm_id, source_dataset_id, source_field, target_dataset_id, target_field, last_event_at) "
              + "ON t.realm_id = v.realm_id "
              + "AND t.source_dataset_id = v.source_dataset_id "
              + "AND t.source_field = v.source_field "
              + "AND t.target_dataset_id = v.target_dataset_id "
              + "AND t.target_field = v.target_field "
              + "WHEN MATCHED THEN UPDATE SET last_event_at = GREATEST(t.last_event_at, v.last_event_at) "
              + "WHEN NOT MATCHED THEN INSERT (realm_id, source_dataset_id, source_field, target_dataset_id, target_field, last_event_at) "
              + "VALUES (v.realm_id, v.source_dataset_id, v.source_field, v.target_dataset_id, v.target_field, v.last_event_at)",
          values);
    }
    return new PreparedQuery(
        "INSERT INTO "
            + table
            + " (realm_id, source_dataset_id, source_field, target_dataset_id, target_field, last_event_at) "
            + "VALUES (?, ?, ?, ?, ?, ?) "
            + "ON CONFLICT (realm_id, source_dataset_id, source_field, target_dataset_id, target_field) DO UPDATE SET "
            + "last_event_at = GREATEST("
            + table
            + ".last_event_at, EXCLUDED.last_event_at)",
        values);
  }

  private static class LineageFieldMappingConverter implements Converter<LineageFieldMapping> {
    @Override
    public LineageFieldMapping fromResultSet(java.sql.ResultSet rs) throws SQLException {
      return new LineageFieldMapping(rs.getString("source_field"), rs.getString("target_field"));
    }

    @Override
    public Map<String, Object> toMap(DatabaseType databaseType) {
      return Map.of();
    }
  }
}
