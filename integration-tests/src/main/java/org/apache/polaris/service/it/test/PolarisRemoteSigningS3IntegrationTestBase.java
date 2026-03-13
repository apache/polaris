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
package org.apache.polaris.service.it.test;

import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_ENDPOINT;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_PATH_STYLE_ACCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.TablePrivilege;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.service.it.env.CatalogConfig;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.RestCatalogConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for S3 remote signing. */
@RestCatalogConfig({"header.X-Iceberg-Access-Delegation", "remote-signing"})
public abstract class PolarisRemoteSigningS3IntegrationTestBase
    extends PolarisRestCatalogS3IntegrationTestBase {

  public static Map<String, String> DEFAULT_REMOTE_SIGNING_CONFIG =
      ImmutableMap.<String, String>builder()
          .put("polaris.readiness.ignore-severe-issues", "true")
          .put("polaris.features.\"REMOTE_SIGNING_ENABLED\"", "true")
          .put("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"S3\",\"FILE\"]")
          .put("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true")
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "false")
          // Enable X-Forwarded-Prefix header support for reverse proxy test
          .put("quarkus.http.proxy.proxy-address-forwarding", "true")
          .put("quarkus.http.proxy.allow-x-forwarded", "true")
          .put("quarkus.http.proxy.enable-forwarded-host", "true")
          .put("quarkus.http.proxy.enable-forwarded-prefix", "true")
          .build();

  private String allowedLocation1;
  private String allowedLocation2;

  @BeforeEach
  @Override
  public void before(TestInfo testInfo) {
    allowedLocation1 = storageBase() + "allowed-location-1/";
    allowedLocation2 = storageBase() + "allowed-location-2/";
    super.before(testInfo);
  }

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    return AwsStorageConfigInfo.builder()
        .setRoleArn(roleArn())
        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
        .setPathStyleAccess(pathStyleAccess())
        .setAllowedLocations(List.of(allowedLocation1, allowedLocation2))
        .setEndpoint(endpoint().orElse(null))
        .setStsUnavailable(stsUnavailable())
        .build();
  }

  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    ImmutableMap.Builder<String, String> builder =
        super.clientFileIOProperties()
            .put(AWS_PATH_STYLE_ACCESS.getPropertyName(), String.valueOf(pathStyleAccess()));
    endpoint().ifPresent(endpoint -> builder.put(AWS_ENDPOINT.getPropertyName(), endpoint));
    return builder;
  }

  /** Returns the base URI for the storage. Must be an S3 URI and end with a slash. */
  protected String storageBase() {
    return BASE_LOCATION;
  }

  protected String roleArn() {
    return ROLE_ARN;
  }

  protected boolean pathStyleAccess() {
    return true;
  }

  protected Optional<String> endpoint() {
    return Optional.empty();
  }

  protected boolean stsUnavailable() {
    return false;
  }

  @Test
  void testRemoteSigningWithStandardTableLocation() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table = catalog.createTable(TABLE, SCHEMA);
    writeDataFile(table);
    readDataFile(table);
  }

  @CatalogConfig(properties = {"polaris.config.allow.unstructured.table.location", "true"})
  @Test
  public void testRemoteSigningWithCustomTableLocation() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    // Note: the custom location must be within one of the catalog's allowed locations,
    // even when unstructured table locations are enabled.
    String customLocation = allowedLocation2 + "custom/tbl1";
    Table table = catalog.buildTable(TABLE, SCHEMA).withLocation(customLocation).create();
    writeDataFile(table);
    readDataFile(table);
  }

  @CatalogConfig(properties = {"polaris.config.allow.unstructured.table.location", "true"})
  @Test
  public void testRemoteSigningWithCustomWritePath() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withProperty("write.data.path", allowedLocation1 + "custom/data")
            .create();
    writeDataFile(table);
    readDataFile(table);
  }

  /**
   * Tests object store layout with "standard" settings. Data files are written to locations inside
   * the table base location, following the pattern {@code
   * <table-base>/data/<file-hash>/[<partition>/]file1.parquet}.
   */
  @Test
  public void testRemoteSigningWithIcebergObjectStoreLayout() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withProperty("write.object-storage.enabled", "true")
            .create();
    writeDataFile(table);
    readDataFile(table);
  }

  /**
   * Tests object store layout with custom write paths. Data files are written to custom locations,
   * outside the table base location, following the pattern {@code
   * <write.data.path>/<file-hash>/<ns>/<table>/[<partition>/]file1.parquet}.
   */
  @CatalogConfig(properties = {"polaris.config.allow.unstructured.table.location", "true"})
  @Test
  public void testRemoteSigningWithCustomWritePathAndIcebergObjectStoreLayout() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withProperty("write.object-storage.enabled", "true")
            .withProperty("write.data.path", allowedLocation1 + "custom/data")
            .create();
    writeDataFile(table);
    readDataFile(table);
  }

  /**
   * Tests Polaris object store layout. The table base location is modified to include a prefix
   * derived from the table name, and data files are written to locations inside the modified base
   * location, following the pattern {@code
   * <warehouse>/<table-hash>/<ns>/<table>/data/[<partition>/]file1.parquet}.
   */
  @CatalogConfig(
      properties = {
        "polaris.config.default-table-location-object-storage-prefix.enabled",
        "true",
        "polaris.config.allow.unstructured.table.location",
        "true",
        "polaris.config.allow.overlapping.table.location",
        "true"
      })
  @Test
  public void testRemoteSigningWithPolarisObjectStoreLayout() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table = catalog.createTable(TABLE, SCHEMA);
    writeDataFile(table);
    readDataFile(table);
  }

  /**
   * Tests Polaris object store layout and Iceberg object store layout both enabled. The table base
   * location is modified to include a prefix derived from the table name, and data files are
   * written to locations inside the modified base location, following the pattern {@code
   * <warehouse>/<table-hash>/<ns>/<table>/data/<file-hash>/[<partition>/]file1.parquet}.
   */
  @CatalogConfig(
      properties = {
        "polaris.config.default-table-location-object-storage-prefix.enabled",
        "true",
        "polaris.config.allow.unstructured.table.location",
        "true",
        "polaris.config.allow.overlapping.table.location",
        "true"
      })
  @Test
  public void testRemoteSigningWithPolarisObjectStoreLayoutAndIcebergObjectStoreLayout()
      throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withProperty("write.object-storage.enabled", "true")
            .create();
    writeDataFile(table);
    readDataFile(table);
  }

  @CatalogConfig(properties = {"polaris.config.remote-signing.enabled", "false"})
  @Test
  public void testRemoteSigningDisabledGlobally() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    assertThatThrownBy(() -> catalog.createTable(TABLE, SCHEMA))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Remote signing is not enabled for this catalog")
        .hasMessageContaining(FeatureConfiguration.REMOTE_SIGNING_ENABLED.key())
        .hasMessageContaining(FeatureConfiguration.REMOTE_SIGNING_ENABLED.catalogConfig());
  }

  @CatalogConfig(Catalog.TypeEnum.EXTERNAL)
  @Test
  public void testRemoteSigningDisabledForExternalCatalog() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            externalCatalogBaseLocation() + "/ns1/ext_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = initializeClientFileIO(new ResolvingFileIO())) {
      String metadataLocation =
          externalCatalogBaseLocation() + "/ns1/ext_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(metadataLocation));
      TableIdentifier extTable = TableIdentifier.of(NS, "ext_table");
      // When the client requests remote-signing access delegation, registering a table
      // on an external catalog should fail because remote signing is not supported.
      assertThatThrownBy(() -> catalog.registerTable(extTable, metadataLocation))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Remote signing is not enabled for external catalogs");
    }
  }

  @Test
  public void testRemoteSigningWithUnsupportedStorageType(@TempDir Path tempDir)
      throws IOException {
    String catalogName = "file-catalog";
    URI catalogRoot = tempDir.resolve("file-catalog").toUri();
    Catalog catalog =
        PolarisCatalog.builder()
            .setName(catalogName)
            .setProperties(CatalogProperties.builder(catalogRoot.toString()).build())
            .setStorageConfigInfo(
                new FileStorageConfigInfo(
                    StorageConfigInfo.StorageTypeEnum.FILE, List.of(catalogRoot.toString()), null))
            .build();
    createPolarisCatalog(catalog);
    makeAdmin(catalog);
    try (RESTCatalog fileCatalog = initCatalog(catalogName, Map.of("warehouse", catalogName))) {
      fileCatalog.createNamespace(NS);
      assertThatThrownBy(() -> fileCatalog.createTable(TABLE, SCHEMA))
          .isInstanceOf(BadRequestException.class)
          .hasMessageContaining("Remote signing is not supported for storage type: FILE");
    }
  }

  /**
   * Test that when X-Forwarded-Prefix header is sent, the signer.uri property in the
   * LoadTableResponse contains the expected prefix.
   */
  @CatalogConfig(properties = {"header.X-Forwarded-Prefix", "/polaris-proxy"})
  @Test
  public void testRemoteSigningWithReverseProxy() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table = catalog.createTable(TABLE, SCHEMA);
    @SuppressWarnings("resource")
    Map<String, String> config = table.io().properties();
    assertThat(config).containsKey(RESTCatalogProperties.SIGNER_URI);
    String signerUri = config.get(RESTCatalogProperties.SIGNER_URI);
    assertThat(signerUri).matches(".*/polaris-proxy/api/catalog/");
  }

  /**
   * Tests that a principal without the TABLE_REMOTE_SIGN privilege on the namespace cannot create a
   * table if it also requests remote signing access delegation.
   */
  @Test
  public void testRemoteSigningCreateTableUnauthorized() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    ClientPrincipal principal =
        createTestPrincipal(
            Set.of(),
            Set.of(
                NamespacePrivilege.TABLE_CREATE,
                NamespacePrivilege.TABLE_FULL_METADATA,
                NamespacePrivilege.TABLE_WRITE_DATA),
            Set.of());
    try (RESTCatalog restrictedCatalog = initCatalog(principal)) {
      assertThatThrownBy(() -> restrictedCatalog.createTable(TableIdentifier.of(NS, "t2"), SCHEMA))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("is not authorized for op REMOTE_SIGN");
    }
  }

  /**
   * Tests that a principal without the TABLE_REMOTE_SIGN privilege on a table cannot load the table
   * if it also requests remote signing access delegation.
   */
  @Test
  public void testRemoteSigningLoadTableUnauthorized() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    catalog.createTable(TABLE, SCHEMA);
    ClientPrincipal principal =
        createTestPrincipal(
            Set.of(),
            Set.of(),
            Set.of(TablePrivilege.TABLE_FULL_METADATA, TablePrivilege.TABLE_WRITE_DATA));
    try (RESTCatalog restrictedCatalog = initCatalog(principal)) {
      assertThatThrownBy(() -> restrictedCatalog.loadTable(TABLE))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("is not authorized for op REMOTE_SIGN");
    }
  }

  /**
   * Tests that a principal with TABLE_REMOTE_SIGN and TABLE_READ_DATA privileges (but not
   * TABLE_WRITE_DATA) can perform read remote signing requests but not write requests.
   */
  @Test
  public void testRemoteSigningWriteUnauthorized() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table = catalog.createTable(TABLE, SCHEMA);
    writeDataFile(table);
    ClientPrincipal principal =
        createTestPrincipal(
            Set.of(),
            Set.of(),
            Set.of(
                TablePrivilege.TABLE_READ_PROPERTIES,
                TablePrivilege.TABLE_READ_DATA,
                TablePrivilege.TABLE_REMOTE_SIGN));
    try (RESTCatalog restrictedCatalog = initCatalog(principal)) {
      Table restrictedTable = restrictedCatalog.loadTable(TABLE);
      assertThatCode(() -> readDataFile(restrictedTable)).doesNotThrowAnyException();
      assertThatThrownBy(() -> writeDataFile(restrictedTable))
          .rootCause()
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Not authorized to sign write request");
    }
  }

  /**
   * Tests that a principal with TABLE_REMOTE_SIGN and TABLE_WRITE_DATA privileges can perform both
   * read and write remote signing requests, since TABLE_WRITE_DATA implies TABLE_READ_DATA.
   */
  @Test
  public void testRemoteSigningWriteImpliesRead() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    catalog.createTable(TABLE, SCHEMA);
    ClientPrincipal principal =
        createTestPrincipal(
            Set.of(),
            Set.of(),
            Set.of(
                TablePrivilege.TABLE_FULL_METADATA,
                TablePrivilege.TABLE_REMOTE_SIGN,
                TablePrivilege.TABLE_WRITE_DATA));
    try (RESTCatalog restrictedCatalog = initCatalog(principal)) {
      Table restrictedTable = restrictedCatalog.loadTable(TABLE);
      writeDataFile(restrictedTable);
      readDataFile(restrictedTable);
    }
  }

  private void writeDataFile(Table table) throws IOException {
    String filepath = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
    @SuppressWarnings("resource")
    OutputFile outputFile = table.io().newOutputFile(filepath);

    DataFile dataFile;
    try (DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(table.spec())
            .build()) {

      for (int i = 0; i < 3; i++) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.set(0, i);
        record.set(1, "test-data-" + i);
        dataWriter.write(record);
      }

      // Close the writer. This triggers remote signing for the data file write.
      dataWriter.close();
      dataFile = dataWriter.toDataFile();
    }

    // Append the data file to the table and commit.
    // This triggers remote signing for manifest and manifest list writes.
    table.newAppend().appendFile(dataFile).commit();

    // Verify that the data file was written.
    // This triggers a scan planning with remote signing read requests
    // for manifests and manifest lists.
    assertFiles(table, dataFile);
  }

  private void readDataFile(Table table) throws IOException {
    // Read the data back from the table, expecting 3 records.
    // This triggers remote signing for manifest and data file reads.
    List<Record> records;
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
      records = Lists.newArrayList(reader);
    }

    // Verify the data content for completeness
    assertThat(records).hasSize(3);
    for (int i = 0; i < 3; i++) {
      Record record = records.get(i);
      assertThat(record.get(0)).isEqualTo(i);
      assertThat(record.get(1)).isEqualTo("test-data-" + i);
    }
  }
}
