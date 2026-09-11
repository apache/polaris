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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TableMetadataTransitionValidatorTest {
  private static final String KEY_ID = TableProperties.ENCRYPTION_TABLE_KEY;

  @Test
  void allowsLegacyTableWithKeyIdToRemainOutsideInvariant() {
    assertThatCode(
            () ->
                TableMetadataTransitionValidator.validate(
                    Map.of(), metadata(Map.of(KEY_ID, "key-1"))))
        .doesNotThrowAnyException();
  }

  @Test
  void allowsLegacyTableWithoutKeyIdToRemainOutsideInvariant() {
    assertThatCode(() -> TableMetadataTransitionValidator.validate(Map.of(), metadata(Map.of())))
        .doesNotThrowAnyException();
  }

  @Test
  void allowsUnchangedPinnedKeyId() {
    assertThatCode(
            () ->
                TableMetadataTransitionValidator.validate(
                    pinned("key-1"), metadata(Map.of(KEY_ID, "key-1"))))
        .doesNotThrowAnyException();
  }

  @Test
  void allowsUnchangedPinnedMissingKeyId() {
    assertThatCode(
            () -> TableMetadataTransitionValidator.validate(pinned(null), metadata(Map.of())))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @MethodSource("changedKeyIds")
  void rejectsChangedPinnedKeyId(String pinnedKeyId, Map<String, String> newProperties) {
    assertThatThrownBy(
            () ->
                TableMetadataTransitionValidator.validate(
                    pinned(pinnedKeyId), metadata(newProperties)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot add, change, or remove encryption key ID after table creation");
  }

  @ParameterizedTest
  @MethodSource("changedKeyIds")
  void rejectsLoadedMetadataWithChangedPinnedKeyId(
      String pinnedKeyId, Map<String, String> loadedProperties) {
    assertThatThrownBy(
            () ->
                TableMetadataTransitionValidator.validateLoaded(
                    pinned(pinnedKeyId), metadata(loadedProperties)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Iceberg table metadata integrity check failed for test.metadata.json: encryption key "
                + "ID does not match trusted catalog state");
  }

  @Test
  void allowsLoadedMetadataWithPinnedMissingKeyId() {
    assertThatCode(
            () -> TableMetadataTransitionValidator.validateLoaded(pinned(null), metadata(Map.of())))
        .doesNotThrowAnyException();
  }

  @Test
  void leavesLoadedLegacyMetadataOutsideInvariant() {
    assertThatCode(
            () ->
                TableMetadataTransitionValidator.validateLoaded(
                    Map.of(), metadata(Map.of(KEY_ID, "key-1"))))
        .doesNotThrowAnyException();
  }

  private static Stream<Arguments> changedKeyIds() {
    return Stream.of(
        Arguments.of(null, Map.of(KEY_ID, "key-1")),
        Arguments.of("key-1", Map.of(KEY_ID, "key-2")),
        Arguments.of("key-1", Map.of()));
  }

  @Test
  void pinsPresentKeyId() {
    Map<String, String> trustedProperties = new HashMap<>();

    TableMetadataTransitionValidator.pin(trustedProperties, metadata(Map.of(KEY_ID, "key-1")));

    assertThat(trustedProperties)
        .containsEntry(TableMetadataTransitionValidator.KEY_ID_PINNED_PROPERTY, "true")
        .containsEntry(TableMetadataTransitionValidator.KEY_ID_PROPERTY, "key-1")
        .doesNotContainKey(KEY_ID);
  }

  @Test
  void pinsMissingKeyId() {
    Map<String, String> trustedProperties =
        new HashMap<>(Map.of(TableMetadataTransitionValidator.KEY_ID_PROPERTY, "old-key"));

    TableMetadataTransitionValidator.pin(trustedProperties, metadata(Map.of()));

    assertThat(trustedProperties)
        .containsEntry(TableMetadataTransitionValidator.KEY_ID_PINNED_PROPERTY, "true")
        .doesNotContainKey(TableMetadataTransitionValidator.KEY_ID_PROPERTY);
  }

  @Test
  void rejectsIncompatibleEncryptionPropertiesBeforePinning() {
    Map<String, String> trustedProperties = new HashMap<>();

    assertThatThrownBy(
            () ->
                TableMetadataTransitionValidator.pin(
                    trustedProperties, metadata(2, Map.of(KEY_ID, "key-1"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid properties for v2")
        .hasMessageContaining(KEY_ID);
    assertThat(trustedProperties).isEmpty();
  }

  private static Map<String, String> pinned(String keyId) {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableMetadataTransitionValidator.KEY_ID_PINNED_PROPERTY, "true");
    if (keyId != null) {
      properties.put(TableMetadataTransitionValidator.KEY_ID_PROPERTY, keyId);
    }
    return properties;
  }

  private static TableMetadata metadata(Map<String, String> properties) {
    return metadata(3, properties);
  }

  private static TableMetadata metadata(int formatVersion, Map<String, String> properties) {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.formatVersion()).thenReturn(formatVersion);
    when(metadata.metadataFileLocation()).thenReturn("test.metadata.json");
    when(metadata.properties()).thenReturn(properties);
    return metadata;
  }
}
