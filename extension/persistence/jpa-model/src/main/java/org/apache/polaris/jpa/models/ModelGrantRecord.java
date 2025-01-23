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
package org.apache.polaris.jpa.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.apache.polaris.core.entity.PolarisGrantRecord;

/**
 * GrantRecord model representing a privilege record of a securable granted to grantee. This is used
 * to exchange the information with GRANT_RECORDS table
 */
@Entity
@Table(
    name = "GRANT_RECORDS",
    indexes = {
      @Index(
          name = "GRANT_RECORDS_BY_GRANTEE_INDEX",
          columnList = "granteeCatalogId,granteeId,securableCatalogId,securableId,privilegeCode")
    })
public class ModelGrantRecord {

  // id of the catalog where the securable entity resides, NULL_ID if this entity is a top-level
  // account entity
  @Id private long securableCatalogId;

  // id of the securable
  @Id private long securableId;

  // id of the catalog where the grantee entity resides, NULL_ID if this entity is a top-level
  // account entity
  @Id private long granteeCatalogId;

  // id of the grantee
  @Id private long granteeId;

  // id associated to the privilege
  @Id private int privilegeCode;

  // Used for Optimistic Locking to handle concurrent reads and updates
  @Version private long version;

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
