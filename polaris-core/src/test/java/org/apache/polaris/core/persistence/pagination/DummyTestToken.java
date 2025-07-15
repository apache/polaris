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
package org.apache.polaris.core.persistence.pagination;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.immutables.PolarisImmutable;

@PolarisImmutable
@JsonSerialize(as = ImmutableDummyTestToken.class)
@JsonDeserialize(as = ImmutableDummyTestToken.class)
public interface DummyTestToken extends Token {
  String ID = "test-dummy";

  Optional<String> s();

  OptionalInt i();

  @Override
  default String getT() {
    return ID;
  }

  final class DummyTestTokenType implements TokenType {
    @Override
    public String id() {
      return ID;
    }

    @Override
    public Class<? extends Token> javaType() {
      return DummyTestToken.class;
    }
  }
}
