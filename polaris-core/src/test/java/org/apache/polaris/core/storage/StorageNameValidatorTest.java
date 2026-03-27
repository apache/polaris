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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class StorageNameValidatorTest {

  @ParameterizedTest
  @ValueSource(strings = {"my-storage", "storage_1", "S3-minio-prod", "a", "A1_b2-c3"})
  void validNames(String name) {
    assertThatNoException().isThrownBy(() -> StorageNameValidator.validate(name));
  }

  @ParameterizedTest
  @NullAndEmptySource
  void nullAndEmptyNames(String name) {
    assertThatThrownBy(() -> StorageNameValidator.validate(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null or empty");
  }

  @ParameterizedTest
  @ValueSource(strings = {"has space", "has.dot", "has/slash", "has@symbol", "has:colon"})
  void invalidCharacters(String name) {
    assertThatThrownBy(() -> StorageNameValidator.validate(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid characters");
  }

  @Test
  void maxLengthAccepted() {
    String name = "a".repeat(StorageNameValidator.MAX_LENGTH);
    assertThatNoException().isThrownBy(() -> StorageNameValidator.validate(name));
  }

  @Test
  void exceedsMaxLength() {
    String name = "a".repeat(StorageNameValidator.MAX_LENGTH + 1);
    assertThatThrownBy(() -> StorageNameValidator.validate(name))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not exceed");
  }

  @Test
  void normalizeBlankToNull_null() {
    assertThat(StorageNameValidator.normalizeBlankToNull(null)).isNull();
  }

  @Test
  void normalizeBlankToNull_empty() {
    assertThat(StorageNameValidator.normalizeBlankToNull("")).isNull();
  }

  @Test
  void normalizeBlankToNull_blank() {
    assertThat(StorageNameValidator.normalizeBlankToNull("   ")).isNull();
  }

  @Test
  void normalizeBlankToNull_trims() {
    assertThat(StorageNameValidator.normalizeBlankToNull("  my-storage  ")).isEqualTo("my-storage");
  }

  @Test
  void normalizeBlankToNull_nonBlank() {
    assertThat(StorageNameValidator.normalizeBlankToNull("my-storage")).isEqualTo("my-storage");
  }
}
