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
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

/**
 * Model class for the metrics schema version table.
 *
 * <p>This is separate from {@link SchemaVersion} which tracks the entity schema version. The
 * metrics schema can evolve independently from the entity schema.
 */
public class MetricsSchemaVersion implements Converter<MetricsSchemaVersion> {

  private final Integer value;

  public MetricsSchemaVersion() {
    this.value = null;
  }

  private MetricsSchemaVersion(int value) {
    this.value = value;
  }

  public int getValue() {
    if (value == null) {
      throw new IllegalStateException(
          "Metrics schema version should be constructed via fromResultSet");
    }
    return value;
  }

  @Override
  public MetricsSchemaVersion fromResultSet(ResultSet rs) throws SQLException {
    return new MetricsSchemaVersion(rs.getInt("version_value"));
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    return Map.of("version_value", this.value);
  }
}
