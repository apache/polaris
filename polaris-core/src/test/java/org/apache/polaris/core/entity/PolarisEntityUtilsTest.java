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
package org.apache.polaris.core.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

public class PolarisEntityUtilsTest {

  static Stream<Arguments> encodeNamespaceCases() {
    return Stream.of(
        // empty namespace encodes to empty string
        Arguments.of(Namespace.empty(), ""),
        // single-level namespaces
        Arguments.of(Namespace.of("a"), "a"),
        Arguments.of(Namespace.of("hello world"), "hello+world"),
        Arguments.of(Namespace.of("a/b"), "a%2Fb"),
        // literal "%1F" in a level name is double-encoded so it can't be confused with separator
        Arguments.of(Namespace.of("%1F"), "%251F"),
        // multi-level namespaces: levels are joined with %1F separator
        Arguments.of(Namespace.of("a", "b"), "a%1Fb"),
        Arguments.of(Namespace.of("a", "b", "c"), "a%1Fb%1Fc"),
        Arguments.of(Namespace.of("a/b", "c"), "a%2Fb%1Fc"),
        // empty level in a multi-level namespace
        Arguments.of(Namespace.of("a", "", "b"), "a%1F%1Fb"));
  }

  @ParameterizedTest
  @MethodSource("encodeNamespaceCases")
  public void testEncodeNamespace(Namespace ns, String expected) {
    assertThat(PolarisEntityUtils.encodeNamespace(ns))
        .isEqualTo(RESTUtil.encodeNamespace(ns))
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @NullSource
  public void testEncodeNamespaceNullInput(Namespace ns) {
    assertThatThrownBy(() -> PolarisEntityUtils.encodeNamespace(ns))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");
  }

  static Stream<Arguments> decodeNamespaceCases() {
    return Stream.of(
        // empty string decodes to a single empty-string level (not Namespace.empty())
        Arguments.of("", Namespace.of("")),
        // single-level namespaces
        Arguments.of("a", Namespace.of("a")),
        Arguments.of("hello+world", Namespace.of("hello world")),
        Arguments.of("a%2Fb", Namespace.of("a/b")),
        // double-encoded %25 decodes back to literal %1F level
        Arguments.of("%251F", Namespace.of("%1F")),
        // multi-level namespaces: split on %1F separator
        Arguments.of("a%1Fb", Namespace.of("a", "b")),
        Arguments.of("a%1Fb%1Fc", Namespace.of("a", "b", "c")),
        Arguments.of("a%2Fb%1Fc", Namespace.of("a/b", "c")),
        // empty level in a multi-level namespace
        Arguments.of("a%1F%1Fb", Namespace.of("a", "", "b")));
  }

  @ParameterizedTest
  @MethodSource("decodeNamespaceCases")
  public void testDecodeNamespace(String encoded, Namespace expected) {
    assertThat(PolarisEntityUtils.decodeNamespace(encoded))
        .isEqualTo(RESTUtil.decodeNamespace(encoded))
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @NullSource
  public void testDecodeNamespaceNullInput(String encoded) {
    assertThatThrownBy(() -> PolarisEntityUtils.decodeNamespace(encoded))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");
  }

  static Stream<Namespace> roundTripCases() {
    return Stream.of(
        Namespace.of("a"),
        Namespace.of("a", "b"),
        Namespace.of("a", "b", "c"),
        Namespace.of("hello world"),
        Namespace.of("a/b", "c"),
        // literal %1F in a level name must survive the round-trip
        Namespace.of("%1F"),
        Namespace.of("a", "", "b"),
        Namespace.of("unicode \u4e2d\u6587"));
  }

  @ParameterizedTest
  @MethodSource("roundTripCases")
  public void testRoundTrip(Namespace ns) {
    assertThat(PolarisEntityUtils.decodeNamespace(PolarisEntityUtils.encodeNamespace(ns)))
        .isEqualTo(ns);
  }
}
