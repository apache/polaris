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
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

public class EntityNameLookupRecordConverter implements Converter<EntityNameLookupRecord> {

  @Override
  public EntityNameLookupRecord fromResultSet(ResultSet rs) throws SQLException {
    return new EntityNameLookupRecord(
        rs.getLong("catalog_id"),
        rs.getLong("id"),
        rs.getLong("parent_id"),
        rs.getString("name"),
        rs.getInt("type_code"),
        rs.getInt("sub_type_code"));
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType, int schemaVersion) {
    throw new UnsupportedOperationException(
        "EntityNameLookupRecordConverter is read-only and does not support toMap operation");
  }
}
