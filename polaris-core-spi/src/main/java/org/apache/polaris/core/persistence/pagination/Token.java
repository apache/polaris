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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import org.immutables.value.Value;

/**
 * Token base interface.
 *
 * <p>Concrete token implementations extend this {@link Token} interface and provide a Java services
 * registered class that implements {@link Token.TokenType}.
 *
 * <p>Serialization property names should be intentionally short to reduce the size of the
 * serialized paging token.
 *
 * <p>Example:
 *
 * {@snippet :
 * @PolarisImmutable
 * @JsonSerialize(as = ImmutableExampleToken.class)
 * @JsonDeserialize(as = ImmutableExampleToken.class)
 * public interface ExampleToken extends Token {
 *   String ID = "example";
 *
 *   @Override
 *   default String getT() {
 *     return ID;
 *   }
 *
 *   @JsonProperty("a")
 *   long a();
 *
 *   @JsonProperty("b")
 *   String b();
 *
 *   static ExampleToken newExampleToken(long a, String b) {
 *     return ImmutableExampleToken.builder().a(a).b(b).build();
 *   }
 *
 *   final class ExampleTokenType implements TokenType {
 *     @Override
 *     public String id() {
 *       return ID;
 *     }
 *
 *     @Override
 *     public Class<? extends Token> javaType() {
 *       return ExampleToken.class;
 *     }
 *   }
 * }
 * }
 *
 * plus a resource file {@code
 * META-INF/services/org.apache.polaris.core.persistence.pagination.Token$TokenType} containing
 * {@code org.apache.polaris.examples.pagetoken.ExampleToken$ExampleTokenType}.
 */
@JsonTypeIdResolver(PageTokenUtil.TokenTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "t", visible = true)
public interface Token {

  @Value.Redacted
  @JsonIgnore
  // must use 'getT' here, otherwise the property won't be properly "wired" to be the type info and
  // Jackson (deserialization) fails with
  // 'com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException: Unrecognized field "t"', if
  // this property is just named 'String t()'
  String getT();

  /** Token type specification, referenced via Java's service loader mechanism. */
  interface TokenType {
    /**
     * ID of the token type, must be equal to the result of {@link Token#getT()} of the concrete
     * {@link #javaType() token type}.
     */
    String id();

    /** Concrete token type. */
    Class<? extends Token> javaType();
  }
}
