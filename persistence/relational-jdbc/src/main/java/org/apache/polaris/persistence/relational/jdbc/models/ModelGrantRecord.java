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
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;

public class ModelGrantRecord implements Converter<PolarisGrantRecord> {
  public static final String TABLE_NAME = "GRANT_RECORDS";

  public static final List<String> ALL_COLUMNS =
      List.of(
          "securable_catalog_id",
          "securable_id",
          "grantee_catalog_id",
          "grantee_id",
          "privilege_code");

  // id of the catalog where the securable entity resides, use 0, if this entity is a
  // top-level account entity.
  private long securableCatalogId;

  // id of the securable
  private long securableId;

  // id of the catalog where the grantee entity resides, use 0, if this entity is a
  // top-level account entity.
  private long granteeCatalogId;

  // id of the grantee
  private long granteeId;

  // id associated to the privilege
  private int privilegeCode;

  public long getSecurableCatalogId() {
    return securableCatalogId;
  }

  public long getSecurableId() {
    return securableId;
  }

  public long getGranteeCatalogId() {
    return granteeCatalogId;
  }

  public long getGranteeId() {
    return granteeId;
  }

  public int getPrivilegeCode() {
    return privilegeCode;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public PolarisGrantRecord fromResultSet(ResultSet rs) throws SQLException {
    var modelGrantRecord =
        ModelGrantRecord.builder()
            .securableCatalogId(rs.getObject("securable_catalog_id", Long.class))
            .securableId(rs.getObject("securable_id", Long.class))
            .granteeCatalogId(rs.getObject("grantee_catalog_id", Long.class))
            .granteeId(rs.getObject("grantee_id", Long.class))
            .privilegeCode(rs.getObject("privilege_code", Integer.class))
            .build();

    return toGrantRecord(modelGrantRecord);
  }

  @Override
  public Map<String, Object> toMap(DatabaseType databaseType) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("securable_catalog_id", this.securableCatalogId);
    map.put("securable_id", this.securableId);
    map.put("grantee_catalog_id", this.granteeCatalogId);
    map.put("grantee_id", this.granteeId);
    map.put("privilege_code", this.privilegeCode);
    return map;
  }

  public static final class Builder {
    private final ModelGrantRecord grantRecord;

    private Builder() {
      grantRecord = new ModelGrantRecord();
    }

    public Builder securableCatalogId(long securableCatalogId) {
      grantRecord.securableCatalogId = securableCatalogId;
      return this;
    }

    public Builder securableId(long securableId) {
      grantRecord.securableId = securableId;
      return this;
    }

    public Builder granteeCatalogId(long granteeCatalogId) {
      grantRecord.granteeCatalogId = granteeCatalogId;
      return this;
    }

    public Builder granteeId(long granteeId) {
      grantRecord.granteeId = granteeId;
      return this;
    }

    public Builder privilegeCode(int privilegeCode) {
      grantRecord.privilegeCode = privilegeCode;
      return this;
    }

    public ModelGrantRecord build() {
      return grantRecord;
    }
  }

  public static ModelGrantRecord fromGrantRecord(PolarisGrantRecord record) {
    if (record == null) return null;

    return ModelGrantRecord.builder()
        .securableCatalogId(record.getSecurableCatalogId())
        .securableId(record.getSecurableId())
        .granteeCatalogId(record.getGranteeCatalogId())
        .granteeId(record.getGranteeId())
        .privilegeCode(record.getPrivilegeCode())
        .build();
  }

  public static PolarisGrantRecord toGrantRecord(ModelGrantRecord model) {
    if (model == null) return null;

    return new PolarisGrantRecord(
        model.getSecurableCatalogId(),
        model.getSecurableId(),
        model.getGranteeCatalogId(),
        model.getGranteeId(),
        model.getPrivilegeCode());
  }
}
