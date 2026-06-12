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
package org.apache.polaris.core.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class StorageNameValidatorTest {

  @Test
  void validNameReturnedVerbatim() {
    assertThat(StorageNameValidator.normalizeAndValidate("team-a_1")).isEqualTo("team-a_1");
    assertThat(StorageNameValidator.normalizeAndValidate("PROD")).isEqualTo("PROD");
    assertThat(StorageNameValidator.normalizeAndValidate("a")).isEqualTo("a");
  }

  @Test
  void surroundingWhitespaceIsTrimmed() {
    assertThat(StorageNameValidator.normalizeAndValidate("  prod  ")).isEqualTo("prod");
    assertThat(StorageNameValidator.normalizeAndValidate("\tprod\n")).isEqualTo("prod");
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"   ", "\t", "\n  \t"})
  void blankNormalizesToNull(String input) {
    assertThat(StorageNameValidator.normalizeAndValidate(input)).isNull();
  }

  @Test
  void overMaxLengthRejected() {
    String tooLong = "a".repeat(StorageNameValidator.MAX_LENGTH + 1);
    assertThatThrownBy(() -> StorageNameValidator.normalizeAndValidate(tooLong))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.valueOf(StorageNameValidator.MAX_LENGTH));
  }

  @Test
  void atMaxLengthAccepted() {
    String maxLen = "a".repeat(StorageNameValidator.MAX_LENGTH);
    assertThat(StorageNameValidator.normalizeAndValidate(maxLen)).isEqualTo(maxLen);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"foo bar", "a/b", "a.b", "a:b", "a#b", "name!", "tab\tname", "with space"})
  void invalidCharactersRejected(String input) {
    assertThatThrownBy(() -> StorageNameValidator.normalizeAndValidate(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("[a-zA-Z0-9_-]");
  }
}
