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
package org.apache.polaris.service.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.catalog.io.PolarisTestKms;
import org.apache.polaris.service.it.env.CatalogConfig;
import org.apache.polaris.service.it.test.PolarisRestCatalogFileIntegrationTest;
import org.apache.polaris.service.task.TaskErrorHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(Profiles.RestCatalogFileIntegrationProfile.class)
public class RestCatalogFileIntegrationTest extends PolarisRestCatalogFileIntegrationTest {
  private static final String TEST_KMS_IMPL =
      "org.apache.polaris.service.catalog.io.PolarisTestKms";
  private static final Schema ENCRYPTED_TABLE_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @Inject
  @Identifier("task-error-handler")
  TaskErrorHandler taskErrorHandler;

  @AfterEach
  void checkTaskExceptions() {
    taskErrorHandler.assertNoTaskExceptions();
  }

  @Test
  @CatalogConfig(
      properties = {org.apache.iceberg.CatalogProperties.ENCRYPTION_KMS_IMPL, TEST_KMS_IMPL})
  void encryptedTableWrittenByClientCanBePurgedByServer() throws IOException {
    Namespace namespace = Namespace.of("encrypted");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");
    catalog().createNamespace(namespace);
    Table restTable =
        catalog()
            .buildTable(identifier, ENCRYPTED_TABLE_SCHEMA)
            .withProperty(TableProperties.FORMAT_VERSION, "3")
            .withProperty(TableProperties.ENCRYPTION_TABLE_KEY, PolarisTestKms.MASTER_KEY_NAME)
            .create();

    TableOperations restOperations = ((HasTableOperations) restTable).operations();
    TableMetadata committedMetadata;
    Snapshot snapshot;
    List<ManifestFile> manifests;
    String dataFileLocation;
    try (KeyManagementClient clientKms = new PolarisTestKms()) {
      EncryptionManager encryptionManager =
          EncryptionUtil.createEncryptionManager(
              restOperations.current().encryptionKeys(),
              restOperations.current().properties(),
              clientKms);
      TableOperations engineOperations =
          new EncryptingCommitTableOperations(restOperations, encryptionManager);
      Table engineTable = new BaseTable(engineOperations, restTable.name());
      EncryptingFileIO engineFileIO = (EncryptingFileIO) engineOperations.io();

      dataFileLocation = engineTable.locationProvider().newDataLocation("encrypted-data.parquet");
      byte[] data = "encrypted table data".getBytes(UTF_8);
      EncryptedOutputFile encryptedData = engineFileIO.newEncryptingOutputFile(dataFileLocation);
      try (PositionOutputStream output = encryptedData.encryptingOutputFile().createOrOverwrite()) {
        output.write(data);
      }

      long dataFileLength = restOperations.io().newInputFile(dataFileLocation).getLength();
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(dataFileLocation)
              .withFileSizeInBytes(dataFileLength)
              .withEncryptionKeyMetadata(
                  EncryptionUtil.setFileLength(
                      encryptedData.keyMetadata().buffer(), dataFileLength))
              .withFormat(FileFormat.PARQUET)
              .withRecordCount(1)
              .build();

      engineTable.newFastAppend().appendFile(dataFile).commit();
      committedMetadata = engineOperations.current();
      snapshot = committedMetadata.currentSnapshot();
      assertThat(snapshot).isNotNull();
      assertThat(committedMetadata.encryptionKeys()).isNotEmpty();

      manifests = snapshot.allManifests(engineOperations.io());
      assertThat(manifests).isNotEmpty();
      assertEncrypted(restOperations.io(), dataFileLocation);
      assertEncrypted(restOperations.io(), snapshot.manifestListLocation());
      manifests.forEach(manifest -> assertEncrypted(restOperations.io(), manifest.path()));
    }

    List<String> filesToDelete = new ArrayList<>();
    filesToDelete.add(dataFileLocation);
    filesToDelete.add(snapshot.manifestListLocation());
    filesToDelete.add(committedMetadata.metadataFileLocation());
    manifests.forEach(manifest -> filesToDelete.add(manifest.path()));

    assertThat(catalog().dropTable(identifier, true)).isTrue();
    assertThat(catalog().tableExists(identifier)).isFalse();
    await("encrypted table files are purged")
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> {
              assertThat(filesToDelete)
                  .filteredOn(location -> restOperations.io().newInputFile(location).exists())
                  .as("remaining table files")
                  .isEmpty();
            });
    taskErrorHandler.assertNoTaskExceptions();
  }

  private static void assertEncrypted(FileIO fileIO, String location) {
    try (SeekableInputStream input = fileIO.newInputFile(location).newStream()) {
      byte[] magic = new byte[4];
      assertThat(input.read(magic)).isEqualTo(magic.length);
      assertThat(magic).containsExactly("AGS1".getBytes(UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Models the client-side REST encryption support while Iceberg PR #13225 is pending. */
  private static final class EncryptingCommitTableOperations implements TableOperations {
    private final TableOperations delegate;
    private final EncryptionManager encryptionManager;
    private final FileIO encryptingFileIO;

    private EncryptingCommitTableOperations(
        TableOperations delegate, EncryptionManager encryptionManager) {
      this.delegate = delegate;
      this.encryptionManager = encryptionManager;
      this.encryptingFileIO = EncryptingFileIO.combine(delegate.io(), encryptionManager);
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
      EncryptionUtil.encryptionKeys(encryptionManager).values().forEach(builder::addEncryptionKey);
      delegate.commit(base, builder.build());
    }

    @Override
    public FileIO io() {
      return encryptingFileIO;
    }

    @Override
    public EncryptionManager encryption() {
      return encryptionManager;
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return delegate.requireStrictCleanup();
    }
  }
}
