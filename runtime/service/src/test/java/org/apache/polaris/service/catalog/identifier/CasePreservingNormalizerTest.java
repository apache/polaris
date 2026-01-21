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
package org.apache.polaris.service.catalog.identifier;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class CasePreservingNormalizerTest {

  private final CasePreservingNormalizer normalizer = CasePreservingNormalizer.INSTANCE;

  @Test
  public void testNormalizeNamePreservesCase() {
    Assertions.assertThat(normalizer.normalizeName("test")).isEqualTo("test");
    Assertions.assertThat(normalizer.normalizeName("TEST")).isEqualTo("TEST");
    Assertions.assertThat(normalizer.normalizeName("Test")).isEqualTo("Test");
    Assertions.assertThat(normalizer.normalizeName("TeSt123")).isEqualTo("TeSt123");
  }

  @Test
  public void testNormalizeNameNull() {
    Assertions.assertThat(normalizer.normalizeName(null)).isNull();
  }

  @Test
  public void testNormalizeNamespaceEmpty() {
    Namespace empty = Namespace.empty();
    Assertions.assertThat(normalizer.normalizeNamespace(empty)).isEqualTo(empty);
  }

  @Test
  public void testNormalizeNamespaceNull() {
    Assertions.assertThat(normalizer.normalizeNamespace(null)).isNull();
  }

  @Test
  public void testNormalizeNamespacePreservesCase() {
    Namespace ns = Namespace.of("MyDatabase", "MySchema");
    Namespace normalized = normalizer.normalizeNamespace(ns);
    Assertions.assertThat(normalized.levels()).containsExactly("MyDatabase", "MySchema");
  }

  @Test
  public void testNormalizeTableIdentifierNull() {
    Assertions.assertThat(normalizer.normalizeTableIdentifier(null)).isNull();
  }

  @Test
  public void testNormalizeTableIdentifierPreservesCase() {
    TableIdentifier id = TableIdentifier.of("MyNamespace", "MyTable");
    TableIdentifier normalized = normalizer.normalizeTableIdentifier(id);
    Assertions.assertThat(normalized.namespace().levels()).containsExactly("MyNamespace");
    Assertions.assertThat(normalized.name()).isEqualTo("MyTable");
  }

  @Test
  public void testNormalizeNamesNull() {
    Assertions.assertThat(normalizer.normalizeNames(null)).isNull();
  }

  @Test
  public void testNormalizeNamesPreservesCase() {
    List<String> names = Arrays.asList("Foo", "BAR", "baz");
    List<String> normalized = normalizer.normalizeNames(names);
    Assertions.assertThat(normalized).containsExactly("Foo", "BAR", "baz");
  }

  @Test
  public void testIsCaseNormalizing() {
    Assertions.assertThat(normalizer.isCaseNormalizing()).isFalse();
  }
}
