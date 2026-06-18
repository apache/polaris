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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

/** Model class for the lineage_column_edges table. */
@PolarisImmutable
public interface ModelLineageColumnEdge extends Converter<ModelLineageColumnEdge> {
  String TABLE_NAME = "LINEAGE_COLUMN_EDGES";

  String REALM_ID = "realm_id";
  String SOURCE_DATASET_ID = "source_dataset_id";
  String SOURCE_FIELD = "source_field";
  String TARGET_DATASET_ID = "target_dataset_id";
  String TARGET_FIELD = "target_field";
  String LAST_EVENT_AT = "last_event_at";

  List<String> ALL_COLUMNS =
      List.of(SOURCE_DATASET_ID, SOURCE_FIELD, TARGET_DATASET_ID, TARGET_FIELD, LAST_EVENT_AT);
  List<String> ALL_COLUMNS_WITH_REALM =
      List.of(
          REALM_ID,
          SOURCE_DATASET_ID,
          SOURCE_FIELD,
          TARGET_DATASET_ID,
          TARGET_FIELD,
          LAST_EVENT_AT);

  String getRealmId();

  long getSourceDatasetId();

  String getSourceField();

  long getTargetDatasetId();

  String getTargetField();

  long getLastEventAt();

  @Override
  default ModelLineageColumnEdge fromResultSet(ResultSet rs) throws SQLException {
    return ImmutableModelLineageColumnEdge.builder()
        .realmId(rs.getString(REALM_ID))
        .sourceDatasetId(rs.getLong(SOURCE_DATASET_ID))
        .sourceField(rs.getString(SOURCE_FIELD))
        .targetDatasetId(rs.getLong(TARGET_DATASET_ID))
        .targetField(rs.getString(TARGET_FIELD))
        .lastEventAt(rs.getLong(LAST_EVENT_AT))
        .build();
  }

  @Override
  default Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(SOURCE_DATASET_ID, getSourceDatasetId());
    map.put(SOURCE_FIELD, getSourceField());
    map.put(TARGET_DATASET_ID, getTargetDatasetId());
    map.put(TARGET_FIELD, getTargetField());
    map.put(LAST_EVENT_AT, getLastEventAt());
    return map;
  }

  static ModelLineageColumnEdge fromIds(
      String realmId,
      long sourceDatasetId,
      String sourceField,
      long targetDatasetId,
      String targetField,
      long lastEventAtMillis) {
    return ImmutableModelLineageColumnEdge.builder()
        .realmId(realmId)
        .sourceDatasetId(sourceDatasetId)
        .sourceField(sourceField)
        .targetDatasetId(targetDatasetId)
        .targetField(targetField)
        .lastEventAt(lastEventAtMillis)
        .build();
  }

  ModelLineageColumnEdge CONVERTER =
      ImmutableModelLineageColumnEdge.builder()
          .realmId("")
          .sourceDatasetId(0L)
          .sourceField("")
          .targetDatasetId(0L)
          .targetField("")
          .lastEventAt(0L)
          .build();
}
