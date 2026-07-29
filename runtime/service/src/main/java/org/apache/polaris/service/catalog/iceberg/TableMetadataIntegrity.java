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

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.util.HashWriter;
import org.apache.iceberg.util.JsonUtil;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;

/** Maintains the trusted metadata digest for encrypted Iceberg tables. */
public final class TableMetadataIntegrity {
  static final String METADATA_HASH_PROPERTY = "polaris.encryption.metadata-hash";
  static final String METADATA_HASH_VERSION_PROPERTY = "polaris.encryption.metadata-hash-version";
  static final String METADATA_HASH_VERSION = "iceberg-canonical-json-sha256-v1";

  private TableMetadataIntegrity() {}

  /**
   * Records the candidate metadata digest in trusted catalog properties.
   *
   * <p>The hash uses Iceberg's canonical JSON representation, matching {@code HiveCatalog}. The
   * digest is only required for encrypted tables.
   */
  static void pin(Map<String, String> trustedProperties, TableMetadata metadata) {
    if (!trustedProperties.containsKey(TableMetadataTransitionValidator.KEY_ID_PINNED_PROPERTY)) {
      return;
    }

    if (metadata.properties().containsKey(TableProperties.ENCRYPTION_TABLE_KEY)) {
      trustedProperties.put(METADATA_HASH_PROPERTY, metadataHash(metadata));
      trustedProperties.put(METADATA_HASH_VERSION_PROPERTY, METADATA_HASH_VERSION);
    } else {
      trustedProperties.remove(METADATA_HASH_PROPERTY);
      trustedProperties.remove(METADATA_HASH_VERSION_PROPERTY);
    }
  }

  /**
   * Verifies encrypted metadata loaded from storage before it is used.
   *
   * <p>Legacy entities without a key-id pin are outside this invariant. Once an entity has been
   * pinned, an encrypted table must also have a matching digest.
   */
  public static void validate(IcebergTableLikeEntity entity, TableMetadata metadata) {
    Map<String, String> trustedProperties = entity.getInternalPropertiesAsMap();
    if (!trustedProperties.containsKey(TableMetadataTransitionValidator.KEY_ID_PINNED_PROPERTY)) {
      return;
    }

    TableMetadataTransitionValidator.validateLoaded(trustedProperties, metadata);

    String trustedKeyId = trustedProperties.get(TableMetadataTransitionValidator.KEY_ID_PROPERTY);
    String expectedHash = trustedProperties.get(METADATA_HASH_PROPERTY);
    String hashVersion = trustedProperties.get(METADATA_HASH_VERSION_PROPERTY);
    if (trustedKeyId == null) {
      if (expectedHash != null || hashVersion != null) {
        throw integrityFailure(metadata, "trusted catalog state is inconsistent");
      }
      return;
    }
    if (expectedHash == null) {
      throw integrityFailure(metadata, "trusted catalog state has no metadata digest");
    }
    if (!METADATA_HASH_VERSION.equals(hashVersion)) {
      throw integrityFailure(
          metadata, "trusted catalog state has an unsupported metadata digest version");
    }

    byte[] expected;
    try {
      expected = Base64.getDecoder().decode(expectedHash);
    } catch (IllegalArgumentException e) {
      throw integrityFailure(metadata, "trusted catalog state has an invalid metadata digest", e);
    }

    if (!MessageDigest.isEqual(expected, metadataHashBytes(metadata))) {
      throw integrityFailure(metadata, "metadata does not match the trusted catalog digest");
    }
  }

  static String metadataHash(TableMetadata metadata) {
    return Base64.getEncoder().encodeToString(metadataHashBytes(metadata));
  }

  private static byte[] metadataHashBytes(TableMetadata metadata) {
    try (HashWriter hashWriter = new HashWriter("SHA-256", StandardCharsets.UTF_8)) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(hashWriter);
      TableMetadataParser.toJson(metadata, generator);
      generator.flush();
      return hashWriter.getHash();
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new IllegalStateException("Unable to hash Iceberg table metadata", e);
    }
  }

  private static TableMetadataIntegrityException integrityFailure(
      TableMetadata metadata, String reason) {
    return integrityFailure(metadata, reason, null);
  }

  private static TableMetadataIntegrityException integrityFailure(
      TableMetadata metadata, String reason, Exception cause) {
    String message =
        "Iceberg table metadata integrity check failed for "
            + metadata.metadataFileLocation()
            + ": "
            + reason;
    return cause == null
        ? new TableMetadataIntegrityException(message)
        : new TableMetadataIntegrityException(message, cause);
  }
}
