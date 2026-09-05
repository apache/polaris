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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.EncryptedKeyParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.polaris.core.entity.PolarisTaskConstants;

/** Prepares and applies the encryption context used by Iceberg cleanup tasks. */
public final class CleanupTaskEncryption {

  private static final String CONTEXT_FORMAT_SMILE_V1 = "EC1";
  private static final ObjectMapper SMILE_MAPPER = SmileMapper.builder().build();

  private CleanupTaskEncryption() {}

  /**
   * Copies the catalog KMS configuration and the table's wrapped encryption keys into a cleanup
   * task. Cleanup tasks run after the catalog request has ended, so they cannot reconstruct these
   * values from the live table operations object.
   */
  public static void setupCleanupTaskEncryptionProperties(
      Map<String, String> taskProperties,
      Map<String, String> catalogProperties,
      TableMetadata metadata) {
    // Task properties start with table metadata. Always discard a table-controlled value for this
    // reserved task field, including when the table is not encrypted.
    taskProperties.remove(PolarisTaskConstants.ENCRYPTION_CONTEXT);

    if (metadata == null
        || metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY) == null) {
      return;
    }

    // Keep trusted catalog KMS settings and wrapped table keys in one task-only envelope. A custom
    // KeyManagementClient may use additional catalog properties, but these values must not be
    // merged into the FileIO and storage properties already resolved for the cleanup task.
    CleanupTaskEncryptionContext context =
        new CleanupTaskEncryptionContext(
            catalogProperties,
            metadata.encryptionKeys().stream().map(EncryptedKeyParser::toJson).toList());
    taskProperties.put(PolarisTaskConstants.ENCRYPTION_CONTEXT, serializeContext(context));
  }

  /**
   * Wraps a task FileIO with Iceberg's EncryptingFileIO when the task represents an encrypted
   * table. The KMS client is owned by the returned FileIO and is closed with it.
   */
  public static FileIO encryptTaskFileIO(FileIO fileIO, Map<String, String> taskProperties) {
    if (taskProperties.get(TableProperties.ENCRYPTION_TABLE_KEY) == null) {
      return fileIO;
    }

    CleanupTaskEncryptionContext context = cleanupTaskEncryptionContext(taskProperties);
    Map<String, String> catalogKmsProperties = context.kmsProperties();
    if (!catalogKmsProperties.containsKey(CatalogProperties.ENCRYPTION_KMS_TYPE)
        && !catalogKmsProperties.containsKey(CatalogProperties.ENCRYPTION_KMS_IMPL)) {
      throw new IllegalArgumentException(
          "Cannot purge an encrypted table without encryption.kms-type or encryption.kms-impl");
    }

    // Like CatalogUtil.loadFileIO, createKmsClient dynamically loads the configured implementation
    // and initializes it with the supplied catalog properties. Credential resolution is delegated
    // to the configured KeyManagementClient.
    KeyManagementClient keyManagementClient = EncryptionUtil.createKmsClient(catalogKmsProperties);
    boolean kmsOwnershipTransferred = false;
    try {
      EncryptionManager encryptionManager =
          new CloseableStandardEncryptionManager(
              encryptedKeys(context),
              taskProperties.get(TableProperties.ENCRYPTION_TABLE_KEY),
              PropertyUtil.propertyAsInt(
                  taskProperties,
                  TableProperties.ENCRYPTION_DEK_LENGTH,
                  TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT),
              keyManagementClient);

      // EncryptingFileIO closes an EncryptionManager when it is Closeable. The subclass preserves
      // StandardEncryptionManager's type because Iceberg uses that type to decrypt manifest-list
      // key metadata, while also closing the task-owned KMS client.
      FileIO encryptingFileIO = EncryptingFileIO.combine(fileIO, encryptionManager);
      kmsOwnershipTransferred = true;
      return encryptingFileIO;
    } finally {
      if (!kmsOwnershipTransferred) {
        keyManagementClient.close();
      }
    }
  }

  private static CleanupTaskEncryptionContext cleanupTaskEncryptionContext(
      Map<String, String> taskProperties) {
    String serializedContext = taskProperties.get(PolarisTaskConstants.ENCRYPTION_CONTEXT);
    if (serializedContext == null) {
      throw new IllegalArgumentException("Missing encryption context in cleanup task properties");
    }
    return deserializeContext(serializedContext);
  }

  static String serializeContext(CleanupTaskEncryptionContext context) {
    try {
      byte[] smile = SMILE_MAPPER.writeValueAsBytes(context);
      return CONTEXT_FORMAT_SMILE_V1
          + Base64.getUrlEncoder().withoutPadding().encodeToString(smile);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize cleanup task encryption context", e);
    }
  }

  static CleanupTaskEncryptionContext deserializeContext(String serializedContext) {
    if (!serializedContext.startsWith(CONTEXT_FORMAT_SMILE_V1)) {
      throw new IllegalArgumentException("Unrecognized cleanup task encryption context format");
    }

    byte[] smile =
        Base64.getUrlDecoder()
            .decode(serializedContext.substring(CONTEXT_FORMAT_SMILE_V1.length()));
    try {
      return SMILE_MAPPER.readValue(smile, CleanupTaskEncryptionContext.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize cleanup task encryption context", e);
    }
  }

  private static List<EncryptedKey> encryptedKeys(CleanupTaskEncryptionContext context) {
    return context.encryptedKeys().stream().map(EncryptedKeyParser::fromJson).toList();
  }

  /**
   * Persisted encryption context for a cleanup task.
   *
   * <p>The component names are part of the persisted serialization contract. They must not be
   * changed without introducing a new format version and a compatible decoder.
   */
  record CleanupTaskEncryptionContext(
      Map<String, String> kmsProperties, List<String> encryptedKeys) {
    CleanupTaskEncryptionContext {
      kmsProperties = Map.copyOf(kmsProperties);
      encryptedKeys = List.copyOf(encryptedKeys);
    }
  }

  /** Makes the task-owned KMS client closeable without hiding Iceberg's standard manager type. */
  private static final class CloseableStandardEncryptionManager extends StandardEncryptionManager
      implements Closeable {
    private final KeyManagementClient keyManagementClient;

    private CloseableStandardEncryptionManager(
        List<EncryptedKey> keys,
        String tableKeyId,
        int dataKeyLength,
        KeyManagementClient keyManagementClient) {
      super(keys, tableKeyId, dataKeyLength, keyManagementClient);
      this.keyManagementClient = keyManagementClient;
    }

    @Override
    public void close() throws IOException {
      keyManagementClient.close();
    }
  }
}
