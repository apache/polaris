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

package org.apache.polaris.core.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NamespaceUtilsTest {

  static Stream<Arguments> joinNamespaceCases() {
    return Stream.of(
        // empty namespace
        Arguments.of(Namespace.of(), ""),
        // single level
        Arguments.of(Namespace.of("simple"), "simple"),
        // multi-level
        Arguments.of(Namespace.of("foo", "bar", "baz"), "foo\u001fbar\u001fbaz"),
        // spaces
        Arguments.of(Namespace.of("foo bar"), "foo bar"),
        Arguments.of(Namespace.of("foo bar", "baz qux"), "foo bar\u001fbaz qux"),
        // plus sign
        Arguments.of(Namespace.of("foo+bar"), "foo+bar"),
        Arguments.of(Namespace.of("a+b", "c+d"), "a+b\u001fc+d"),
        // percent-encoded sequences (treated as literals, not decoded)
        Arguments.of(Namespace.of("foo%20bar"), "foo%20bar"),
        Arguments.of(Namespace.of("%2F"), "%2F"),
        // slash in level names (safe with the default \u001f separator)
        Arguments.of(Namespace.of("a/b"), "a/b"),
        Arguments.of(Namespace.of("foo/bar", "baz/qux"), "foo/bar\u001fbaz/qux"),
        Arguments.of(Namespace.of("path/to/resource"), "path/to/resource"),
        // non-ASCII characters
        Arguments.of(Namespace.of("café"), "café"),
        Arguments.of(Namespace.of("日本語"), "日本語"),
        Arguments.of(Namespace.of("日本語", "namespace"), "日本語\u001fnamespace"),
        // combination of various special characters
        Arguments.of(
            Namespace.of("foo bar", "baz+qux", "café", "%20"),
            "foo bar\u001fbaz+qux\u001fcafé\u001f%20"));
  }

  @ParameterizedTest
  @MethodSource("joinNamespaceCases")
  void joinNamespace(Namespace namespace, String expected) {
    assertThat(NamespaceUtils.joinNamespace(namespace, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR))
        .isEqualTo(expected);
  }

  static Stream<Arguments> splitNamespaceCases() {
    return Stream.of(
        // single level
        Arguments.of("simple", Namespace.of("simple")),
        // multi-level
        Arguments.of("foo\u001fbar\u001fbaz", Namespace.of("foo", "bar", "baz")),
        // spaces
        Arguments.of("foo bar", Namespace.of("foo bar")),
        Arguments.of("foo bar\u001fbaz qux", Namespace.of("foo bar", "baz qux")),
        // plus sign
        Arguments.of("foo+bar", Namespace.of("foo+bar")),
        Arguments.of("a+b\u001fc+d", Namespace.of("a+b", "c+d")),
        // percent-encoded sequences (treated as literals, not decoded)
        Arguments.of("foo%20bar", Namespace.of("foo%20bar")),
        Arguments.of("%2F", Namespace.of("%2F")),
        // slash in level names (safe with the default \u001f separator)
        Arguments.of("a/b", Namespace.of("a/b")),
        Arguments.of("foo/bar\u001fbaz/qux", Namespace.of("foo/bar", "baz/qux")),
        Arguments.of("path/to/resource", Namespace.of("path/to/resource")),
        // non-ASCII characters
        Arguments.of("café", Namespace.of("café")),
        Arguments.of("日本語", Namespace.of("日本語")),
        Arguments.of("日本語\u001fnamespace", Namespace.of("日本語", "namespace")),
        // combination of various special characters
        Arguments.of(
            "foo bar\u001fbaz+qux\u001fcafé\u001f%20",
            Namespace.of("foo bar", "baz+qux", "café", "%20")));
  }

  @ParameterizedTest
  @MethodSource("splitNamespaceCases")
  void splitNamespace(String joined, Namespace expected) {
    assertThat(NamespaceUtils.splitNamespace(joined, NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR))
        .isEqualTo(expected);
  }

  // Round-trip cases: namespace levels contain none of the tested separators (\u001f, /, |, --).
  static Stream<Arguments> roundTripCases() {
    List<Namespace> namespaces =
        List.of(
            Namespace.of("simple"),
            Namespace.of("foo", "bar", "baz"),
            Namespace.of("foo bar"),
            Namespace.of("foo bar", "baz qux"),
            Namespace.of("foo+bar"),
            Namespace.of("a+b", "c+d"),
            Namespace.of("foo%20bar"),
            Namespace.of("%2F"),
            Namespace.of("café"),
            Namespace.of("日本語"),
            Namespace.of("日本語", "namespace"),
            Namespace.of("foo bar", "baz+qux", "café", "%20"));
    List<String> separators = List.of("\u001f", "/", "|", "--");
    return namespaces.stream().flatMap(ns -> separators.stream().map(sep -> Arguments.of(ns, sep)));
  }

  // Round-trip cases for namespaces whose levels contain "/": only non-"/" separators are valid.
  static Stream<Arguments> roundTripCasesWithSlash() {
    List<Namespace> namespaces =
        List.of(
            Namespace.of("a/b"),
            Namespace.of("foo/bar", "baz/qux"),
            Namespace.of("path/to/resource"),
            Namespace.of("a/b", "c+d", "café"));
    List<String> separators = List.of("\u001f", "|", "--");
    return namespaces.stream().flatMap(ns -> separators.stream().map(sep -> Arguments.of(ns, sep)));
  }

  @ParameterizedTest
  @MethodSource("roundTripCases")
  void roundTrip(Namespace namespace, String separator) {
    assertThat(
            NamespaceUtils.splitNamespace(
                NamespaceUtils.joinNamespace(namespace, separator), separator))
        .isEqualTo(namespace);
  }

  @ParameterizedTest
  @MethodSource("roundTripCasesWithSlash")
  void roundTripWithSlash(Namespace namespace, String separator) {
    assertThat(
            NamespaceUtils.splitNamespace(
                NamespaceUtils.joinNamespace(namespace, separator), separator))
        .isEqualTo(namespace);
  }
}
