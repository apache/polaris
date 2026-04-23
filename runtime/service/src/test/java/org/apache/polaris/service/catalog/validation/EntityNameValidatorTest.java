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

package org.apache.polaris.service.catalog.validation;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class EntityNameValidatorTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "simple",
        "with space",
        "under_score",
        "hy-phen",
        "with.dot",
        "café",
        "日本語",
        "a+b",
        "percent%20encoded",
      })
  void validateNameAccepts(String name) {
    assertThatCode(() -> EntityNameValidator.validateName(name)).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @NullAndEmptySource
  void validateNameRejectsNullOrEmpty(String name) {
    assertThatThrownBy(() -> EntityNameValidator.validateName(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty");
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "a/b", "/leading", "trailing/", "a/b/c"})
  void validateNameRejectsSlash(String name) {
    assertThatThrownBy(() -> EntityNameValidator.validateName(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'/'");
  }

  @ParameterizedTest
  @ValueSource(strings = {" lead", "trail ", " both ", "\ttab", "line\n", " ", "\t", "\n"})
  void validateNameRejectsSurroundingWhitespace(String name) {
    assertThatThrownBy(() -> EntityNameValidator.validateName(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("whitespace");
  }

  @Test
  void validateNamespaceAcceptsEmpty() {
    assertThatCode(() -> EntityNameValidator.validateNamespace(Namespace.empty()))
        .doesNotThrowAnyException();
  }

  @Test
  void validateNamespaceAcceptsMultiLevel() {
    assertThatCode(
            () -> EntityNameValidator.validateNamespace(Namespace.of("ns1", "ns2", "with space")))
        .doesNotThrowAnyException();
  }

  @Test
  void validateNamespaceRejectsLevelWithSlash() {
    assertThatThrownBy(
            () -> EntityNameValidator.validateNamespace(Namespace.of("ns1", "bad/level")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'/'");
  }

  @Test
  void validateNamespaceRejectsEmptyLevel() {
    assertThatThrownBy(() -> EntityNameValidator.validateNamespace(Namespace.of("ns1", "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty");
  }

  @Test
  void validateNamespaceRejectsLevelWithSurroundingWhitespace() {
    assertThatThrownBy(() -> EntityNameValidator.validateNamespace(Namespace.of("ns1", " bad ")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("whitespace");
  }

  @Test
  void validateIdentifierAccepts() {
    assertThatCode(
            () ->
                EntityNameValidator.validateIdentifier(
                    TableIdentifier.of(Namespace.of("ns1", "ns2"), "table")))
        .doesNotThrowAnyException();
  }

  @Test
  void validateIdentifierRejectsBadNamespaceLevel() {
    assertThatThrownBy(
            () ->
                EntityNameValidator.validateIdentifier(
                    TableIdentifier.of(Namespace.of("ns1", "bad/level"), "table")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'/'");
  }

  @Test
  void validateIdentifierRejectsBadName() {
    assertThatThrownBy(
            () ->
                EntityNameValidator.validateIdentifier(
                    TableIdentifier.of(Namespace.of("ns1"), " bad ")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("whitespace");
  }
}
