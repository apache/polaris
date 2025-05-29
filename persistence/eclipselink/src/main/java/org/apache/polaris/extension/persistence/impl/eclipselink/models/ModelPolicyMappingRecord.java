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
package org.apache.polaris.extension.persistence.impl.eclipselink.models;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.eclipse.persistence.annotations.PrimaryKey;

@Entity
@Table(
    name = "POLICY_MAPPING_RECORDS",
    indexes = {
      @Index(
          name = "POLICY_MAPPING_RECORDS_BY_POLICY_INDEX",
          columnList = "policyTypeCode,policyCatalogId,policyId,targetCatalogId,targetId")
    })
@PrimaryKey(
    columns = {
      @Column(name = "targetCatalogId"),
      @Column(name = "targetId"),
      @Column(name = "policyTypeCode"),
      @Column(name = "policyCatalogId"),
      @Column(name = "policyId")
    })
public class ModelPolicyMappingRecord {
  // id of the catalog where target entity resides
  @Id private long targetCatalogId;

  // id of the target entity
  @Id private long targetId;

  // id associated to the policy type
  @Id private int policyTypeCode;

  // id of the catalog where the policy entity resides
  @Id private long policyCatalogId;

  // id of the policy
  @Id private long policyId;

  // additional parameters of the mapping
  private String parameters;

  // Used for Optimistic Locking to handle concurrent reads and updates
  @Version private long version;

  public long getTargetCatalogId() {
    return targetCatalogId;
  }

  public long getTargetId() {
    return targetId;
  }

  public int getPolicyTypeCode() {
    return policyTypeCode;
  }

  public long getPolicyCatalogId() {
    return policyCatalogId;
  }

  public long getPolicyId() {
    return policyId;
  }

  public String getParameters() {
    return parameters;
  }

  public static ModelPolicyMappingRecord.Builder builder() {
    return new ModelPolicyMappingRecord.Builder();
  }

  public static final class Builder {
    private final ModelPolicyMappingRecord policyMappingRecord;

    private Builder() {
      policyMappingRecord = new ModelPolicyMappingRecord();
    }

    public Builder targetCatalogId(long targetCatalogId) {
      policyMappingRecord.targetCatalogId = targetCatalogId;
      return this;
    }

    public Builder targetId(long targetId) {
      policyMappingRecord.targetId = targetId;
      return this;
    }

    public Builder policyTypeCode(int policyTypeCode) {
      policyMappingRecord.policyTypeCode = policyTypeCode;
      return this;
    }

    public Builder policyCatalogId(long policyCatalogId) {
      policyMappingRecord.policyCatalogId = policyCatalogId;
      return this;
    }

    public Builder policyId(long policyId) {
      policyMappingRecord.policyId = policyId;
      return this;
    }

    public Builder parameters(String parameters) {
      policyMappingRecord.parameters = parameters;
      return this;
    }

    public ModelPolicyMappingRecord build() {
      return policyMappingRecord;
    }
  }

  public void update(PolarisPolicyMappingRecord record) {
    this.targetCatalogId = record.getTargetCatalogId();
    this.targetId = record.getTargetId();
    this.policyTypeCode = record.getPolicyTypeCode();
    this.policyCatalogId = record.getPolicyCatalogId();
    this.policyId = record.getPolicyId();
    this.parameters = record.getParameters();
  }

  public static ModelPolicyMappingRecord fromPolicyMappingRecord(
      PolarisPolicyMappingRecord record) {
    if (record == null) return null;

    return ModelPolicyMappingRecord.builder()
        .targetCatalogId(record.getTargetCatalogId())
        .targetId(record.getTargetId())
        .policyTypeCode(record.getPolicyTypeCode())
        .policyCatalogId(record.getPolicyCatalogId())
        .policyId(record.getPolicyId())
        .parameters(record.getParameters())
        .build();
  }

  public static PolarisPolicyMappingRecord toPolicyMappingRecord(ModelPolicyMappingRecord model) {
    if (model == null) return null;

    return new PolarisPolicyMappingRecord(
        model.getTargetCatalogId(),
        model.getTargetId(),
        model.getPolicyCatalogId(),
        model.getPolicyId(),
        model.getPolicyTypeCode(),
        model.getParameters());
  }
}
