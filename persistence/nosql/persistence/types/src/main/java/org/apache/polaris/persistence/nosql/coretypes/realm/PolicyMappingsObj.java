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
package org.apache.polaris.persistence.nosql.coretypes.realm;

import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarLong;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.nio.ByteBuffer;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

@PolarisImmutable
@JsonSerialize(as = ImmutablePolicyMappingsObj.class)
@JsonDeserialize(as = ImmutablePolicyMappingsObj.class)
public interface PolicyMappingsObj extends BaseCommitObj {

  String POLICY_MAPPINGS_REF_NAME = "policy-mappings";

  ObjType TYPE = new PolicyMappingsObjType();

  IndexContainer<PolicyMapping> policyMappings();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutablePolicyMappingsObj.Builder builder() {
    return ImmutablePolicyMappingsObj.builder();
  }

  final class PolicyMappingsObjType extends AbstractObjType<PolicyMappingsObj> {
    public PolicyMappingsObjType() {
      super("polmap", "PolicyMappings", PolicyMappingsObj.class);
    }
  }

  interface Builder extends BaseCommitObj.Builder<PolicyMappingsObj, Builder> {
    @CanIgnoreReturnValue
    Builder from(PolicyMappingsObj container);

    @CanIgnoreReturnValue
    Builder policyMappings(IndexContainer<PolicyMapping> policyMappings);
  }

  //  - key by entity:
  //    - E/entityCatalogId/entityId/policyType/policyCatalogId/policyId
  //      VALUE: PolicyMapping(parameters)
  //  - key by policy:
  //    - P/policyType/policyCatalogId/policyId/entityCatalogId/entityId
  //      VALUE: (empty)

  interface PolicyMappingKey {
    static PolicyMappingKey fromIndexKey(IndexKey key) {
      var buffer = key.asByteBuffer();
      var type = buffer.get();
      return switch (type) {
        case 'E' -> KeyByEntity.fromBuffer(buffer);
        case 'P' -> KeyByPolicy.fromBuffer(buffer);
        default ->
            throw new IllegalArgumentException("Invalid policy mapping key type: " + (char) type);
      };
    }

    PolicyMappingKey reverse();

    IndexKey toIndexKey();

    PolarisPolicyMappingRecord toMappingRecord(PolicyMapping value);
  }

  record KeyByEntity(
      long entityCatalogId, long entityId, int policyType, long policyCatalogId, long policyId)
      implements PolicyMappingKey {
    static KeyByEntity fromBuffer(ByteBuffer buffer) {
      var entityCatalogId = readVarLong(buffer);
      var entityId = readVarLong(buffer);
      var policyType = readVarInt(buffer);
      var policyCatalogId = readVarLong(buffer);
      var policyId = readVarLong(buffer);
      return new KeyByEntity(entityCatalogId, entityId, policyType, policyCatalogId, policyId);
    }

    @Override
    public PolicyMappingKey reverse() {
      return new KeyByPolicy(policyCatalogId, policyId, policyType, entityCatalogId, entityId);
    }

    @Override
    public IndexKey toIndexKey() {
      var buffer = ByteBuffer.allocate(1 + 5 * 9);
      buffer.put((byte) 'E');
      putVarInt(buffer, entityCatalogId);
      putVarInt(buffer, entityId);
      putVarInt(buffer, policyType);
      putVarInt(buffer, policyCatalogId);
      putVarInt(buffer, policyId);
      buffer.flip();
      return IndexKey.key(buffer);
    }

    public IndexKey toEntityPartialIndexKey() {
      var buffer = ByteBuffer.allocate(1 + 2 * 9);
      buffer.put((byte) 'E');
      putVarInt(buffer, entityCatalogId);
      putVarInt(buffer, entityId);
      buffer.flip();
      return IndexKey.key(buffer);
    }

    public IndexKey toPolicyTypePartialIndexKey() {
      var buffer = ByteBuffer.allocate(1 + 3 * 9);
      buffer.put((byte) 'E');
      putVarInt(buffer, entityCatalogId);
      putVarInt(buffer, entityId);
      putVarInt(buffer, policyType);
      buffer.flip();
      return IndexKey.key(buffer);
    }

    @Override
    public PolarisPolicyMappingRecord toMappingRecord(PolicyMapping value) {
      return new PolarisPolicyMappingRecord(
          entityCatalogId, entityId, policyCatalogId, policyId, policyType, value.parameters());
    }
  }

  record KeyByPolicy(
      long policyCatalogId, long policyId, int policyType, long entityCatalogId, long entityId)
      implements PolicyMappingKey {
    static KeyByPolicy fromBuffer(ByteBuffer buffer) {
      var policyCatalogId = readVarLong(buffer);
      var policyId = readVarLong(buffer);
      var policyType = readVarInt(buffer);
      var entityCatalogId = readVarLong(buffer);
      var entityId = readVarLong(buffer);
      return new KeyByPolicy(policyCatalogId, policyId, policyType, entityCatalogId, entityId);
    }

    @Override
    public PolicyMappingKey reverse() {
      return new KeyByEntity(entityCatalogId, entityId, policyType, policyCatalogId, policyId);
    }

    @Override
    public IndexKey toIndexKey() {
      var buffer = ByteBuffer.allocate(1 + 5 * 9);
      buffer.put((byte) 'P');
      putVarInt(buffer, policyCatalogId);
      putVarInt(buffer, policyId);
      putVarInt(buffer, policyType);
      putVarInt(buffer, entityCatalogId);
      putVarInt(buffer, entityId);
      buffer.flip();
      return IndexKey.key(buffer);
    }

    public IndexKey toPolicyPartialIndexKey() {
      var buffer = ByteBuffer.allocate(1 + 2 * 9);
      buffer.put((byte) 'P');
      putVarInt(buffer, policyCatalogId);
      putVarInt(buffer, policyId);
      buffer.flip();
      return IndexKey.key(buffer);
    }

    public IndexKey toPolicyWithTypePartialIndexKey() {
      var buffer = ByteBuffer.allocate(1 + 3 * 9);
      buffer.put((byte) 'P');
      putVarInt(buffer, policyCatalogId);
      putVarInt(buffer, policyId);
      putVarInt(buffer, policyType);
      buffer.flip();
      return IndexKey.key(buffer);
    }

    @Override
    public PolarisPolicyMappingRecord toMappingRecord(PolicyMapping value) {
      return new PolarisPolicyMappingRecord(
          entityCatalogId, entityId, policyCatalogId, policyId, policyType, value.parameters());
    }
  }
}
