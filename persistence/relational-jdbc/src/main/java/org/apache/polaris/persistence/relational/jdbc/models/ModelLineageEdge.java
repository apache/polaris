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
import org.apache.polaris.core.persistence.lineage.LineageEdgeRecord;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

/** Model class for the lineage_edges table. */
@PolarisImmutable
public interface ModelLineageEdge extends Converter<ModelLineageEdge> {
  String TABLE_NAME = "LINEAGE_EDGES";

  String REALM_ID = "realm_id";
  String SOURCE_DATASET_ID = "source_dataset_id";
  String TARGET_DATASET_ID = "target_dataset_id";
  String LAST_EVENT_AT = "last_event_at";

  List<String> ALL_COLUMNS = List.of(SOURCE_DATASET_ID, TARGET_DATASET_ID, LAST_EVENT_AT);
  List<String> ALL_COLUMNS_WITH_REALM =
      List.of(REALM_ID, SOURCE_DATASET_ID, TARGET_DATASET_ID, LAST_EVENT_AT);

  String getRealmId();

  long getSourceDatasetId();

  long getTargetDatasetId();

  long getLastEventAt();

  @Override
  default ModelLineageEdge fromResultSet(ResultSet rs) throws SQLException {
    return ImmutableModelLineageEdge.builder()
        .realmId(rs.getString(REALM_ID))
        .sourceDatasetId(rs.getLong(SOURCE_DATASET_ID))
        .targetDatasetId(rs.getLong(TARGET_DATASET_ID))
        .lastEventAt(rs.getLong(LAST_EVENT_AT))
        .build();
  }

  @Override
  default Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(SOURCE_DATASET_ID, getSourceDatasetId());
    map.put(TARGET_DATASET_ID, getTargetDatasetId());
    map.put(LAST_EVENT_AT, getLastEventAt());
    return map;
  }

  static ModelLineageEdge fromRecord(LineageEdgeRecord record, String realmId) {
    return ImmutableModelLineageEdge.builder()
        .realmId(realmId)
        .sourceDatasetId(record.sourceDatasetId())
        .targetDatasetId(record.targetDatasetId())
        .lastEventAt(record.lastEventAt())
        .build();
  }

  ModelLineageEdge CONVERTER =
      ImmutableModelLineageEdge.builder()
          .realmId("")
          .sourceDatasetId(0L)
          .targetDatasetId(0L)
          .lastEventAt(0L)
          .build();
}
