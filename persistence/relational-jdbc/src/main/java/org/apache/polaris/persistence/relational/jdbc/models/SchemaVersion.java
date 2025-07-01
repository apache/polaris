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

public class SchemaVersion implements Converter<SchemaVersion> {
  private final Integer value;

  public SchemaVersion() {
    this.value = null;
  }

  private SchemaVersion(int value) {
    this.value = value;
  }

  public int getValue() {
    if (value == null) {
      throw new IllegalStateException("Schema version should be constructed via fromResultSet");
    }
    return value;
  }

  @Override
  public SchemaVersion fromResultSet(ResultSet rs) throws SQLException {
    return new SchemaVersion(rs.getInt("version_value"));
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    return Map.of("version_value", this.value);
  }
}
