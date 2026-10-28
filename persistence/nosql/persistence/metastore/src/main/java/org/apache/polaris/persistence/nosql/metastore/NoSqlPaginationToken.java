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

package org.apache.polaris.persistence.nosql.metastore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.ByteBuffer;
import org.apache.polaris.core.persistence.pagination.Token;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.immutables.value.Value;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * Pagination token for NoSQL that refers to the next {@link IndexKey}. The next request will refer
 * to the same index, for example, the same catalog state.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableNoSqlPaginationToken.class)
@JsonDeserialize(as = ImmutableNoSqlPaginationToken.class)
@com.fasterxml.jackson.databind.annotation.JsonSerialize(as = ImmutableNoSqlPaginationToken.class)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(as = ImmutableNoSqlPaginationToken.class)
public interface NoSqlPaginationToken extends Token {
  String ID = "n";

  // This implementation decouples the direct use of the `ObjRef` and `IndexKey` and with
  // that their Jackson version specific (de)serializers by using "neutral" byte buffers.
  // Once both `:polaris-core` and `:polaris-persistence-nosql-*` are both using Jackson 3, the
  // change that introduced the `ByteBuffer` indirection can optionally be reverted.

  @JsonProperty("c")
  ByteBuffer containerObjRefBytes();

  @JsonIgnore
  @Value.Lazy
  default ObjRef containerObjRef() {
    return ObjRef.fromByteBuffer(containerObjRefBytes().duplicate());
  }

  @JsonProperty("k")
  ByteBuffer keyBytes();

  @JsonIgnore
  @Value.Lazy
  default IndexKey key() {
    return IndexKey.key(keyBytes().duplicate());
  }

  @Override
  default String getT() {
    return ID;
  }

  static NoSqlPaginationToken paginationToken(ObjRef containerObjRef, IndexKey key) {
    return ImmutableNoSqlPaginationToken.builder()
        .containerObjRefBytes(containerObjRef.toByteBuffer())
        .keyBytes(key.asByteBuffer())
        .build();
  }

  final class NoSqlPaginationTokenType implements Token.TokenType {
    @Override
    public String id() {
      return ID;
    }

    @Override
    public Class<? extends Token> javaType() {
      return NoSqlPaginationToken.class;
    }
  }
}
