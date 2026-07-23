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
package org.apache.polaris.service.catalog.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.BaseEncryptedKey;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PolarisEncryptionUtilTest {
  private static final String TABLE_KEY = PolarisTestKms.MASTER_KEY_NAME;

  @AfterEach
  void resetKmsState() {
    PolarisTestKms.resetClosed();
  }

  @Test
  void cleanupTaskPropertiesRoundTripEncryptedKeys() {
    EncryptedKey key =
        new BaseEncryptedKey(
            "data-key", ByteBuffer.wrap(new byte[] {1, 2, 3}), TABLE_KEY, Map.of("p", "v"));
    TableMetadata metadata = encryptedMetadata(key);
    Map<String, String> taskProperties = new HashMap<>(metadata.properties());

    PolarisEncryptionUtil.setupCleanupTaskEncryptionProperties(
        taskProperties,
        Map.of(CatalogProperties.ENCRYPTION_KMS_IMPL, PolarisTestKms.class.getName()),
        metadata);

    assertThat(taskProperties).containsKey(PolarisTaskConstants.ENCRYPTION_CONTEXT);
    PolarisEncryptionUtil.CleanupTaskEncryptionContext context = encryptionContext(taskProperties);
    assertThat(context.kmsProperties())
        .containsExactlyEntriesOf(
            Map.of(CatalogProperties.ENCRYPTION_KMS_IMPL, PolarisTestKms.class.getName()));
    assertThat(context.encryptedKeys()).hasSize(1);
  }

  @Test
  void cleanupTaskKmsPropertiesDoNotOverrideTaskProperties() throws IOException {
    TableMetadata metadata = encryptedMetadata();
    Map<String, String> taskProperties = new HashMap<>(metadata.properties());
    taskProperties.put(CatalogProperties.FILE_IO_IMPL, "restricted.FileIO");
    taskProperties.put(CatalogProperties.ENCRYPTION_KMS_IMPL, "untrusted.Kms");
    taskProperties.put("storage.region", "restricted-region");
    taskProperties.put(PolarisTaskConstants.ENCRYPTION_CONTEXT, "untrusted-value");

    PolarisEncryptionUtil.setupCleanupTaskEncryptionProperties(
        taskProperties,
        Map.of(
            CatalogProperties.FILE_IO_IMPL,
            "catalog.FileIO",
            CatalogProperties.ENCRYPTION_KMS_IMPL,
            PolarisTestKms.class.getName(),
            "storage.region",
            "catalog-region",
            "vault-proxy.uri",
            "http://vault-proxy"),
        metadata);

    assertThat(taskProperties)
        .containsEntry(CatalogProperties.FILE_IO_IMPL, "restricted.FileIO")
        .containsEntry(CatalogProperties.ENCRYPTION_KMS_IMPL, "untrusted.Kms")
        .containsEntry("storage.region", "restricted-region");
    PolarisEncryptionUtil.CleanupTaskEncryptionContext context = encryptionContext(taskProperties);
    assertThat(context.kmsProperties())
        .containsEntry(CatalogProperties.FILE_IO_IMPL, "catalog.FileIO")
        .containsEntry(CatalogProperties.ENCRYPTION_KMS_IMPL, PolarisTestKms.class.getName())
        .containsEntry("storage.region", "catalog-region")
        .containsEntry("vault-proxy.uri", "http://vault-proxy")
        .doesNotContainKey("table-controlled-option");

    try (FileIO fileIO =
        PolarisEncryptionUtil.encryptTaskFileIO(new InMemoryFileIO(), taskProperties)) {
      assertThat(fileIO).isInstanceOf(EncryptingFileIO.class);
    }
  }

  @Test
  void encryptedTaskFileIoClosesKmsClient() {
    EncryptedKey key =
        new BaseEncryptedKey(
            "data-key", ByteBuffer.wrap(new byte[] {1, 2, 3}), TABLE_KEY, Map.of());
    TableMetadata metadata = encryptedMetadata(key);
    Map<String, String> taskProperties = new HashMap<>(metadata.properties());
    PolarisEncryptionUtil.setupCleanupTaskEncryptionProperties(
        taskProperties,
        Map.of(CatalogProperties.ENCRYPTION_KMS_IMPL, PolarisTestKms.class.getName()),
        metadata);

    try (var fileIO =
        PolarisEncryptionUtil.encryptTaskFileIO(new InMemoryFileIO(), taskProperties)) {
      assertThat(fileIO).isInstanceOf(EncryptingFileIO.class);
    }

    assertThat(PolarisTestKms.wasClosed()).isTrue();
  }

  @Test
  void encryptedTaskFileIoClosesKmsClientWhenTaskKeyIsInvalid() {
    TableMetadata metadata = encryptedMetadata();
    Map<String, String> taskProperties = new HashMap<>(metadata.properties());
    PolarisEncryptionUtil.setupCleanupTaskEncryptionProperties(
        taskProperties,
        Map.of(CatalogProperties.ENCRYPTION_KMS_IMPL, PolarisTestKms.class.getName()),
        metadata);
    PolarisEncryptionUtil.CleanupTaskEncryptionContext context = encryptionContext(taskProperties);
    taskProperties.put(
        PolarisTaskConstants.ENCRYPTION_CONTEXT,
        PolarisObjectMapperUtil.serialize(
            new PolarisEncryptionUtil.CleanupTaskEncryptionContext(
                context.kmsProperties(), List.of("not-an-encrypted-key"))));

    assertThatThrownBy(
            () -> PolarisEncryptionUtil.encryptTaskFileIO(new InMemoryFileIO(), taskProperties))
        .isInstanceOf(RuntimeException.class);

    assertThat(PolarisTestKms.wasClosed()).isTrue();
  }

  @Test
  void encryptedTaskFileIoRoundTripsEncryptedBytes() throws IOException {
    InMemoryFileIO rawFileIO = new InMemoryFileIO();
    TableMetadata metadata = encryptedMetadata();
    EncryptionManager writerManager =
        EncryptionUtil.createEncryptionManager(
            metadata.encryptionKeys(), metadata.properties(), new PolarisTestKms());
    EncryptingFileIO writerFileIO = EncryptingFileIO.combine(rawFileIO, writerManager);
    String location = "memory:encrypted-data";
    byte[] expected = "encrypted payload".getBytes(StandardCharsets.UTF_8);

    EncryptedOutputFile encryptedOutput = writerFileIO.newEncryptingOutputFile(location);
    try (PositionOutputStream output = encryptedOutput.encryptingOutputFile().createOrOverwrite()) {
      output.write(expected);
    }

    TableMetadata metadataWithKeys = addEncryptionKeys(metadata, writerManager);
    Map<String, String> taskProperties = new HashMap<>(metadataWithKeys.properties());
    PolarisEncryptionUtil.setupCleanupTaskEncryptionProperties(
        taskProperties,
        Map.of(CatalogProperties.ENCRYPTION_KMS_IMPL, PolarisTestKms.class.getName()),
        metadataWithKeys);

    try (FileIO taskFileIO = PolarisEncryptionUtil.encryptTaskFileIO(rawFileIO, taskProperties)) {
      EncryptingFileIO encryptedTaskFileIO = (EncryptingFileIO) taskFileIO;
      assertThat(encryptedTaskFileIO.encryptionManager())
          .isInstanceOf(StandardEncryptionManager.class);
      InputFile rawInput = rawFileIO.newInputFile(location);
      try (SeekableInputStream input = rawInput.newStream()) {
        byte[] magic = new byte[4];
        assertThat(input.read(magic)).isEqualTo(magic.length);
        assertThat(new String(magic, StandardCharsets.UTF_8)).isEqualTo("AGS1");
      }

      InputFile decrypted =
          encryptedTaskFileIO.newDecryptingInputFile(
              location,
              rawInput.getLength(),
              EncryptionUtil.setFileLength(
                  encryptedOutput.keyMetadata().buffer(), rawInput.getLength()));
      try (SeekableInputStream input = decrypted.newStream()) {
        assertThat(input.readAllBytes()).containsExactly(expected);
      }
    }

    assertThat(PolarisTestKms.wasClosed()).isTrue();
  }

  private static PolarisEncryptionUtil.CleanupTaskEncryptionContext encryptionContext(
      Map<String, String> taskProperties) {
    return PolarisObjectMapperUtil.deserialize(
        taskProperties.get(PolarisTaskConstants.ENCRYPTION_CONTEXT),
        PolarisEncryptionUtil.CleanupTaskEncryptionContext.class);
  }

  private static TableMetadata encryptedMetadata() {
    return TableMetadata.newTableMetadata(
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
        org.apache.iceberg.PartitionSpec.unpartitioned(),
        "file:///tmp/encrypted-table",
        Map.of(
            TableProperties.FORMAT_VERSION,
            "3",
            TableProperties.ENCRYPTION_TABLE_KEY,
            PolarisTestKms.MASTER_KEY_NAME,
            TableProperties.ENCRYPTION_DEK_LENGTH,
            "16"));
  }

  private static TableMetadata encryptedMetadata(EncryptedKey key) {
    return addEncryptionKey(encryptedMetadata(), key);
  }

  private static TableMetadata addEncryptionKey(TableMetadata metadata, EncryptedKey key) {
    return TableMetadata.buildFrom(metadata).addEncryptionKey(key).build();
  }

  private static TableMetadata addEncryptionKeys(
      TableMetadata metadata, EncryptionManager encryptionManager) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
    EncryptionUtil.encryptionKeys(encryptionManager).values().forEach(builder::addEncryptionKey);
    return builder.build();
  }
}
