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
import org.junitpioneer.jupiter.params.IntRangeSource;

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
        .hasMessageContaining("Entity name must not be empty");
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "a/b", "/leading", "trailing/", "a/b/c"})
  void validateNameRejectsSlash(String name) {
    assertThatThrownBy(() -> EntityNameValidator.validateName(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Entity name must not contain '/'");
  }

  @ParameterizedTest
  @ValueSource(strings = {" lead", "trail ", " both ", " "})
  void validateNameRejectsSurroundingWhitespace(String name) {
    assertThatThrownBy(() -> EntityNameValidator.validateName(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Entity name must not have leading or trailing whitespace");
  }

  @ParameterizedTest
  @IntRangeSource(from = 0x00, to = 0x1F)
  void validateNameRejectsControlC0Characters(int codePoint) {
    assertThatThrownBy(() -> EntityNameValidator.validateName("name" + (char) codePoint))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Entity name must not contain control characters (U+%04X): name\\u%04X",
            codePoint, codePoint);
  }

  @ParameterizedTest
  @IntRangeSource(from = 0x7F, to = 0x9F)
  void validateNameRejectsControlC1Characters(int codePoint) {
    assertThatThrownBy(() -> EntityNameValidator.validateName("name" + (char) codePoint))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Entity name must not contain control characters (U+%04X): name\\u%04X",
            codePoint, codePoint);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "back\\slash",
        "co:lon",
        "as*terisk",
        "ques?tion",
        "quo\"te",
        "les<s",
        "grea>ter",
        "pi|pe",
        "ha#sh",
        "plu+s",
        "back`tick",
      })
  void validateNameRejectsForbiddenChars(String name) {
    assertThatThrownBy(() -> EntityNameValidator.validateName(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Entity name must not contain '");
  }

  @ParameterizedTest
  @ValueSource(strings = {".", ".."})
  void validateNameRejectsDotOrDotDot(String name) {
    assertThatThrownBy(() -> EntityNameValidator.validateName(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Entity name must not be '.' or '..'");
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
        .hasMessageContaining("Entity name must not contain '/'");
  }

  @Test
  void validateNamespaceRejectsEmptyLevel() {
    assertThatThrownBy(() -> EntityNameValidator.validateNamespace(Namespace.of("ns1", "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Entity name must not be empty");
  }

  @Test
  void validateNamespaceRejectsLevelWithSurroundingWhitespace() {
    assertThatThrownBy(() -> EntityNameValidator.validateNamespace(Namespace.of("ns1", " bad ")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Entity name must not have leading or trailing whitespace");
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
        .hasMessageContaining("Entity name must not contain '/'");
  }

  @Test
  void validateIdentifierRejectsBadName() {
    assertThatThrownBy(
            () ->
                EntityNameValidator.validateIdentifier(
                    TableIdentifier.of(Namespace.of("ns1"), " bad ")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Entity name must not have leading or trailing whitespace");
  }
}
