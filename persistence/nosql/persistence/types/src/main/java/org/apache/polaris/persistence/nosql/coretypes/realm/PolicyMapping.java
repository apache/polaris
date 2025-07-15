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

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

@PolarisImmutable
@JsonSerialize(as = ImmutablePolicyMapping.class)
@JsonDeserialize(as = ImmutablePolicyMapping.class)
public interface PolicyMapping {
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> parameters();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<ObjRef> externalMapping();

  static ImmutablePolicyMapping.Builder builder() {
    return ImmutablePolicyMapping.builder();
  }

  PolicyMapping EMPTY = builder().parameters(Map.of()).build();

  IndexValueSerializer<PolicyMapping> POLICY_MAPPING_SERIALIZER =
      new IndexValueSerializer<>() {
        @Override
        public int serializedSize(@Nullable PolicyMapping value) {
          if (value == null) {
            value = EMPTY;
          }

          var len = 0;

          var params = value.parameters();
          len += varIntLen(params.size());
          for (Map.Entry<String, String> e : params.entrySet()) {
            len += IndexKey.key(e.getKey()).serializedSize();
            len += IndexKey.key(e.getValue()).serializedSize();
          }

          var ext = value.externalMapping();
          len++;
          if (ext.isPresent()) {
            len += OBJ_REF_SERIALIZER.serializedSize(ext.get());
          }

          return len;
        }

        @Override
        @Nonnull
        public ByteBuffer serialize(@Nullable PolicyMapping value, @Nonnull ByteBuffer target) {
          if (value == null) {
            value = EMPTY;
          }

          var params = value.parameters();
          putVarInt(target, params.size());
          params.forEach(
              (k, v) -> {
                IndexKey.key(k).serialize(target);
                IndexKey.key(v).serialize(target);
              });

          var ext = value.externalMapping();
          if (ext.isPresent()) {
            target.put((byte) 1);
            OBJ_REF_SERIALIZER.serialize(ext.get(), target);
          } else {
            target.put((byte) 0);
          }

          return target;
        }

        @Override
        public PolicyMapping deserialize(@Nonnull ByteBuffer buffer) {
          var builder = builder();
          var num = readVarInt(buffer);
          for (int i = 0; i < num; i++) {
            var k = IndexKey.deserializeKey(buffer).toString();
            var v = IndexKey.deserializeKey(buffer).toString();
            builder.putParameter(k, v);
          }

          if (buffer.get() == 1) {
            builder.externalMapping(OBJ_REF_SERIALIZER.deserialize(buffer));
          }

          return builder.build();
        }

        @Override
        public void skip(@Nonnull ByteBuffer buffer) {
          var num = readVarInt(buffer);
          for (int i = 0; i < num; i++) {
            IndexKey.skip(buffer);
            IndexKey.skip(buffer);
          }

          if (buffer.get() == 1) {
            OBJ_REF_SERIALIZER.skip(buffer);
          }
        }
      };
}
