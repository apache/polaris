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
import java.util.Map;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

/** Read-only converter for {@link ModelEntity#VERSION_COLUMNS} projection queries. */
public class EntityVersionConverter implements Converter<EntityVersionConverter.EntityVersionRow> {

  public record EntityVersionRow(
      PolarisEntityId entityId, PolarisChangeTrackingVersions versions) {}

  @Override
  public EntityVersionRow fromResultSet(ResultSet rs) throws SQLException {
    return new EntityVersionRow(
        new PolarisEntityId(rs.getLong("catalog_id"), rs.getLong("id")),
        new PolarisChangeTrackingVersions(
            rs.getInt("entity_version"), rs.getInt("grant_records_version")));
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    throw new UnsupportedOperationException(
        "EntityVersionConverter is read-only and does not support toMap");
  }
}
