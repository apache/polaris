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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.BaseEncryptedKey;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.junit.jupiter.api.Test;

class TableMetadataIntegrityTest {
  private static final String KEY_ID = "test-key";

  @Test
  void pinsAndValidatesEncryptedMetadata() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    Map<String, String> trustedProperties = pinnedProperties(KEY_ID);

    TableMetadataIntegrity.pin(trustedProperties, metadata);

    assertThat(trustedProperties)
        .containsEntry(
            TableMetadataIntegrity.METADATA_HASH_PROPERTY,
            TableMetadataIntegrity.metadataHash(metadata))
        .containsEntry(
            TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY,
            TableMetadataIntegrity.METADATA_HASH_VERSION);
    assertThatCode(() -> TableMetadataIntegrity.validate(entity(trustedProperties), metadata))
        .doesNotThrowAnyException();
  }

  @Test
  void doesNotPinPlaintextMetadata() {
    Map<String, String> trustedProperties = pinnedProperties(null);
    trustedProperties.put(TableMetadataIntegrity.METADATA_HASH_PROPERTY, "obsolete");

    TableMetadataIntegrity.pin(trustedProperties, metadata(Map.of()));

    assertThat(trustedProperties).doesNotContainKey(TableMetadataIntegrity.METADATA_HASH_PROPERTY);
    assertThat(trustedProperties)
        .doesNotContainKey(TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY);
    assertThatCode(
            () -> TableMetadataIntegrity.validate(entity(trustedProperties), metadata(Map.of())))
        .doesNotThrowAnyException();
  }

  @Test
  void doesNotEnrollLegacyEncryptedMetadata() {
    Map<String, String> trustedProperties = new HashMap<>();

    TableMetadataIntegrity.pin(
        trustedProperties, metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID)));

    assertThat(trustedProperties)
        .doesNotContainKey(TableMetadataIntegrity.METADATA_HASH_PROPERTY)
        .doesNotContainKey(TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY)
        .doesNotContainKey(TableMetadataTransitionValidator.KEY_ID_PROPERTY);
  }

  @Test
  void rejectsKeyIdAddedToPinnedPlaintextMetadata() {
    Map<String, String> trustedProperties = pinnedProperties(null);
    TableMetadata modified = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, "added-key"));

    assertThatThrownBy(() -> TableMetadataIntegrity.validate(entity(trustedProperties), modified))
        .isInstanceOf(TableMetadataIntegrityException.class)
        .hasMessageContaining("Iceberg table metadata integrity check failed for")
        .hasMessageContaining(": encryption key ID does not match trusted catalog state");
  }

  @Test
  void rejectsModifiedEncryptedMetadata() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    Map<String, String> trustedProperties = pinnedProperties(KEY_ID);
    TableMetadataIntegrity.pin(trustedProperties, metadata);
    TableMetadata modified =
        metadata(
            Map.of(
                TableProperties.ENCRYPTION_TABLE_KEY,
                KEY_ID,
                TableProperties.COMMIT_NUM_RETRIES,
                "1"));

    assertThatThrownBy(() -> TableMetadataIntegrity.validate(entity(trustedProperties), modified))
        .isInstanceOf(TableMetadataIntegrityException.class)
        .hasMessageContaining("metadata does not match the trusted catalog digest");
  }

  @Test
  void rejectsMissingOrInvalidDigestForPinnedEncryptedTable() {
    Map<String, String> missingDigest = pinnedProperties(KEY_ID);

    assertThatThrownBy(
            () ->
                TableMetadataIntegrity.validate(
                    entity(missingDigest),
                    metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID))))
        .isInstanceOf(TableMetadataIntegrityException.class)
        .hasMessageContaining("trusted catalog state has no metadata digest");

    Map<String, String> invalidDigest = pinnedProperties(KEY_ID);
    invalidDigest.put(TableMetadataIntegrity.METADATA_HASH_PROPERTY, "not-base64!");
    invalidDigest.put(
        TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY,
        TableMetadataIntegrity.METADATA_HASH_VERSION);
    assertThatThrownBy(
            () ->
                TableMetadataIntegrity.validate(
                    entity(invalidDigest),
                    metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID))))
        .isInstanceOf(TableMetadataIntegrityException.class)
        .hasMessageContaining("trusted catalog state has an invalid metadata digest");
  }

  @Test
  void rejectsUnsupportedDigestVersion() {
    TableMetadata metadata = metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID));
    Map<String, String> trustedProperties = pinnedProperties(KEY_ID);
    trustedProperties.put(
        TableMetadataIntegrity.METADATA_HASH_PROPERTY,
        TableMetadataIntegrity.metadataHash(metadata));
    trustedProperties.put(TableMetadataIntegrity.METADATA_HASH_VERSION_PROPERTY, "future-version");

    assertThatThrownBy(() -> TableMetadataIntegrity.validate(entity(trustedProperties), metadata))
        .isInstanceOf(TableMetadataIntegrityException.class)
        .hasMessageContaining("trusted catalog state has an unsupported metadata digest version");
  }

  @Test
  void leavesLegacyUnpinnedEntityOutsideInvariant() {
    assertThatCode(
            () ->
                TableMetadataIntegrity.validate(
                    entity(Map.of()),
                    metadata(Map.of(TableProperties.ENCRYPTION_TABLE_KEY, KEY_ID))))
        .doesNotThrowAnyException();
  }

  @Test
  void canonicalHashIsStableAcrossRealisticMetadataFileRoundTrip() {
    TableMetadata metadata = realisticEncryptedMetadata();
    InMemoryFileIO fileIO = new InMemoryFileIO();
    String location = "file:/tmp/table/metadata/v1.metadata.json";

    assertThat(metadata.snapshots()).hasSize(2);
    assertThat(metadata.snapshotLog()).hasSize(2);
    assertThat(metadata.previousFiles()).hasSize(2);
    assertThat(metadata.statisticsFiles()).hasSize(1);
    assertThat(metadata.partitionStatisticsFiles()).hasSize(1);
    assertThat(metadata.encryptionKeys()).hasSize(2);
    assertThat(metadata.nextRowId()).isEqualTo(15L);

    TableMetadataParser.write(metadata, fileIO.newOutputFile(location));
    TableMetadata parsed = TableMetadataParser.read(fileIO, location);

    assertThat(TableMetadataIntegrity.metadataHash(parsed))
        .isEqualTo(TableMetadataIntegrity.metadataHash(metadata));
  }

  @Test
  void rejectsModifiedEncryptionKeys() {
    TableMetadata metadata = realisticEncryptedMetadata();
    Map<String, String> trustedProperties = pinnedProperties(KEY_ID);
    TableMetadataIntegrity.pin(trustedProperties, metadata);

    TableMetadata modified =
        TableMetadata.buildFrom(metadata)
            .addEncryptionKey(
                new BaseEncryptedKey(
                    "data-key-3",
                    ByteBuffer.wrap(new byte[] {7, 8, 9}),
                    KEY_ID,
                    Map.of("algorithm", "AES-GCM")))
            .build();

    assertThatThrownBy(() -> TableMetadataIntegrity.validate(entity(trustedProperties), modified))
        .isInstanceOf(TableMetadataIntegrityException.class)
        .hasMessageContaining("metadata does not match the trusted catalog digest");
  }

  @Test
  void canonicalHashV1MatchesGoldenMetadata() throws IOException {
    String resource = "/table-metadata-integrity-v1.json";
    String json;
    try (InputStream input = TableMetadataIntegrityTest.class.getResourceAsStream(resource)) {
      assertThat(input).as(resource).isNotNull();
      json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }

    TableMetadata metadata = TableMetadataParser.fromJson(json);

    assertThat(metadata.schemas()).hasSize(2);
    assertThat(metadata.specs()).hasSize(1);
    assertThat(metadata.sortOrders()).hasSize(1);
    assertThat(metadata.refs()).containsKeys("main", "audit");
    assertThat(metadata.statisticsFiles().getFirst().blobMetadata()).hasSize(1);
    assertThat(metadata.encryptionKeys()).hasSize(2);
    // This pins the v1 contract across Iceberg upgrades. Do not regenerate it mechanically.
    assertThat(TableMetadataIntegrity.metadataHash(metadata))
        .isEqualTo("C0vt/i/y4k0B6anvnLNc+vHQSqgO4qcAxP1tCssbyAg=");
  }

  private static TableMetadata realisticEncryptedMetadata() {
    TableMetadata base =
        metadata(
            Map.of(
                TableProperties.ENCRYPTION_TABLE_KEY,
                KEY_ID,
                TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
                "10"));

    TableMetadata withFirstMetadataLogEntry =
        TableMetadata.buildFrom(base)
            .setPreviousFileLocation("file:/tmp/table/metadata/v0.metadata.json")
            .setProperties(Map.of("test.revision", "1"))
            .build();

    Snapshot firstSnapshot =
        SnapshotParser.fromJson(
            """
            {
              "sequence-number": 1,
              "snapshot-id": 101,
              "timestamp-ms": 1000,
              "summary": {
                "operation": "append",
                "added-data-files": "1"
              },
              "manifest-list": "file:/tmp/table/metadata/snap-101.avro",
              "schema-id": 0,
              "first-row-id": 0,
              "added-rows": 10,
              "key-id": "data-key-1"
            }
            """);

    Snapshot secondSnapshot =
        SnapshotParser.fromJson(
            """
            {
              "sequence-number": 2,
              "snapshot-id": 102,
              "parent-snapshot-id": 101,
              "timestamp-ms": 2000,
              "summary": {
                "operation": "append",
                "added-data-files": "1"
              },
              "manifest-list": "file:/tmp/table/metadata/snap-102.avro",
              "schema-id": 0,
              "first-row-id": 10,
              "added-rows": 5,
              "key-id": "data-key-2"
            }
            """);

    TableMetadata withFirstSnapshot =
        TableMetadataParser.fromJson(
            TableMetadataParser.toJson(
                TableMetadata.buildFrom(withFirstMetadataLogEntry)
                    .setBranchSnapshot(firstSnapshot, SnapshotRef.MAIN_BRANCH)
                    .build()));

    return TableMetadata.buildFrom(withFirstSnapshot)
        .setPreviousFileLocation("file:/tmp/table/metadata/v1.metadata.json")
        .setBranchSnapshot(secondSnapshot, SnapshotRef.MAIN_BRANCH)
        .setStatistics(
            new GenericStatisticsFile(
                102L, "file:/tmp/table/metadata/stats-102.puffin", 128L, 1L, List.of()))
        .setPartitionStatistics(
            ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(102L)
                .path("file:/tmp/table/metadata/partition-stats-102.parquet")
                .fileSizeInBytes(256L)
                .build())
        .addEncryptionKey(
            new BaseEncryptedKey(
                "data-key-1",
                ByteBuffer.wrap(new byte[] {1, 2, 3}),
                KEY_ID,
                Map.of("algorithm", "AES-GCM")))
        .addEncryptionKey(
            new BaseEncryptedKey(
                "data-key-2",
                ByteBuffer.wrap(new byte[] {4, 5, 6}),
                KEY_ID,
                Map.of("algorithm", "AES-GCM")))
        .build();
  }

  private static Map<String, String> pinnedProperties(String keyId) {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableMetadataTransitionValidator.KEY_ID_PINNED_PROPERTY, "true");
    if (keyId != null) {
      properties.put(TableMetadataTransitionValidator.KEY_ID_PROPERTY, keyId);
    }
    return properties;
  }

  private static TableMetadata metadata(Map<String, String> properties) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Map<String, String> tableProperties = new HashMap<>(properties);
    if (tableProperties.containsKey(TableProperties.ENCRYPTION_TABLE_KEY)) {
      tableProperties.put(TableProperties.FORMAT_VERSION, "3");
    }
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), "file:/tmp/table", tableProperties);
  }

  private static IcebergTableLikeEntity entity(Map<String, String> properties) {
    return new IcebergTableLikeEntity.Builder(
            PolarisEntitySubType.ICEBERG_TABLE,
            TableIdentifier.of("namespace", "table"),
            Map.of(),
            properties,
            "file:/tmp/table/metadata/v1.metadata.json")
        .build();
  }
}
