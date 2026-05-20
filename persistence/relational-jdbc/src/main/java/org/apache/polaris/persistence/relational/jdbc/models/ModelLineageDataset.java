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
package org.apache.polaris.persistence.relational.jdbc.models;

import jakarta.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.persistence.lineage.LineageDatasetRecord;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

/** Model class for the lineage_datasets table. */
@PolarisImmutable
public interface ModelLineageDataset extends Converter<ModelLineageDataset> {
  String TABLE_NAME = "LINEAGE_DATASETS";

  String REALM_ID = "realm_id";
  String DATASET_ID = "dataset_id";
  String CATALOG = "catalog";
  String NAMESPACE = "namespace";
  String NAME = "name";
  String POLARIS_ENTITY_ID = "polaris_entity_id";
  String CREATED_AT = "created_at";
  String UPDATED_AT = "updated_at";

  List<String> ALL_COLUMNS =
      List.of(DATASET_ID, CATALOG, NAMESPACE, NAME, POLARIS_ENTITY_ID, CREATED_AT, UPDATED_AT);
  List<String> ALL_COLUMNS_WITH_REALM =
      List.of(
          REALM_ID,
          DATASET_ID,
          CATALOG,
          NAMESPACE,
          NAME,
          POLARIS_ENTITY_ID,
          CREATED_AT,
          UPDATED_AT);

  String getRealmId();

  long getDatasetId();

  String getCatalog();

  String getNamespace();

  String getName();

  @Nullable
  Long getPolarisEntityId();

  long getCreatedAt();

  long getUpdatedAt();

  @Override
  default ModelLineageDataset fromResultSet(ResultSet rs) throws SQLException {
    return ImmutableModelLineageDataset.builder()
        .realmId(rs.getString(REALM_ID))
        .datasetId(rs.getLong(DATASET_ID))
        .catalog(rs.getString(CATALOG))
        .namespace(rs.getString(NAMESPACE))
        .name(rs.getString(NAME))
        .polarisEntityId(rs.getObject(POLARIS_ENTITY_ID, Long.class))
        .createdAt(rs.getLong(CREATED_AT))
        .updatedAt(rs.getLong(UPDATED_AT))
        .build();
  }

  @Override
  default Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(DATASET_ID, getDatasetId());
    map.put(CATALOG, getCatalog());
    map.put(NAMESPACE, getNamespace());
    map.put(NAME, getName());
    map.put(POLARIS_ENTITY_ID, getPolarisEntityId());
    map.put(CREATED_AT, getCreatedAt());
    map.put(UPDATED_AT, getUpdatedAt());
    return map;
  }

  static ModelLineageDataset fromRecord(LineageDatasetRecord record, String realmId) {
    return ImmutableModelLineageDataset.builder()
        .realmId(realmId)
        .datasetId(record.datasetId())
        .catalog(record.catalog())
        .namespace(record.namespace())
        .name(record.name())
        .polarisEntityId(record.polarisEntityId().orElse(null))
        .createdAt(record.createdAt())
        .updatedAt(record.updatedAt())
        .build();
  }

  ModelLineageDataset CONVERTER =
      ImmutableModelLineageDataset.builder()
          .realmId("")
          .datasetId(0L)
          .catalog("")
          .namespace("")
          .name("")
          .createdAt(0L)
          .updatedAt(0L)
          .build();
}
