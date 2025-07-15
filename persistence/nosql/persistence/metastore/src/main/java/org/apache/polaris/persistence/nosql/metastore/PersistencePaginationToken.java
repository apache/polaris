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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.core.persistence.pagination.Token;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/**
 * Pagination token for NoSQL that refers to the next {@link
 * org.apache.polaris.persistence.nosql.api.index.IndexKey}. The next request will refer to the same
 * index, for example, the same catalog state.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutablePersistencePaginationToken.class)
@JsonDeserialize(as = ImmutablePersistencePaginationToken.class)
public interface PersistencePaginationToken extends Token {
  String ID = "n";

  @JsonProperty("c")
  ObjRef containerObjRef();

  @JsonProperty("k")
  IndexKey key();

  @Override
  default String getT() {
    return ID;
  }

  static PersistencePaginationToken paginationToken(ObjRef containerObjRef, IndexKey key) {
    return ImmutablePersistencePaginationToken.builder()
        .containerObjRef(containerObjRef)
        .key(key)
        .build();
  }

  final class PersistencePaginationTokenType implements Token.TokenType {
    @Override
    public String id() {
      return ID;
    }

    @Override
    public Class<? extends Token> javaType() {
      return PersistencePaginationToken.class;
    }
  }
}
