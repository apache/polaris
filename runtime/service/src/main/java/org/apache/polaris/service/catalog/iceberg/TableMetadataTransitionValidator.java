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

import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.exceptions.ValidationException;

final class TableMetadataTransitionValidator {
  static final String KEY_ID_PINNED_PROPERTY = "polaris.encryption.key-id-pinned";
  static final String KEY_ID_PROPERTY = "polaris.encryption.key-id";

  private TableMetadataTransitionValidator() {}

  static void validate(Map<String, String> trustedProperties, TableMetadata candidate) {
    if (!trustedProperties.containsKey(KEY_ID_PINNED_PROPERTY)) {
      // Legacy tables remain outside the invariant until an explicit trusted attestation
      // mechanism enrolls them. Ordinary legacy operations must not establish trust from
      // metadata that may have been read from unprotected storage.
      return;
    }
    if (!keyIdMatches(trustedProperties, candidate)) {
      throw new ValidationException(
          "Cannot add, change, or remove encryption key ID after table creation");
    }
  }

  /**
   * Verifies metadata loaded from storage against the trusted catalog pin before it is used.
   *
   * <p>Legacy entities without a pin remain outside the invariant. For pinned entities, a missing
   * key ID is a value in its own right and must continue to be missing.
   */
  static void validateLoaded(
      Map<String, String> trustedProperties, TableMetadata metadataFromStorage) {
    if (!trustedProperties.containsKey(KEY_ID_PINNED_PROPERTY)) {
      return;
    }
    if (!keyIdMatches(trustedProperties, metadataFromStorage)) {
      throw new TableMetadataIntegrityException(
          "Iceberg table metadata integrity check failed for "
              + metadataFromStorage.metadataFileLocation()
              + ": encryption key ID does not match trusted catalog state");
    }
  }

  static void pin(Map<String, String> trustedProperties, TableMetadata metadata) {
    EncryptionUtil.checkCompatibility(metadata.properties(), metadata.formatVersion());
    trustedProperties.put(KEY_ID_PINNED_PROPERTY, Boolean.TRUE.toString());
    String keyId = metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (keyId == null) {
      trustedProperties.remove(KEY_ID_PROPERTY);
    } else {
      trustedProperties.put(KEY_ID_PROPERTY, keyId);
    }
  }

  private static boolean keyIdMatches(
      Map<String, String> trustedProperties, TableMetadata candidate) {
    String expectedKeyId = trustedProperties.get(KEY_ID_PROPERTY);
    String candidateKeyId = candidate.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    return Objects.equals(expectedKeyId, candidateKeyId);
  }
}
