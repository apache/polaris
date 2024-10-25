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
package org.apache.polaris.service;

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.apache.polaris.service.throttling.RequestThrottlingErrorResponse.RequestThrottlingErrorType.REQUEST_TOO_LARGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.ImmutableAuthConfig;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.service.auth.BasePolarisAuthenticator;
import org.apache.polaris.service.catalog.PolarisTestClient;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.SnowmanCredentialsExtension;
import org.apache.polaris.service.throttling.RequestThrottlingErrorResponse;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@ExtendWith({
  DropwizardExtensionsSupport.class,
  PolarisConnectionExtension.class,
  SnowmanCredentialsExtension.class
})
public class PolarisApplicationIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisApplicationIntegrationTest.class);

  public static final String PRINCIPAL_ROLE_NAME = "admin";
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config(
              "server.adminConnectors[0].port", "0")); // Bind to random port to support parallelism

  private static String userToken;
  private static SnowmanCredentialsExtension.SnowmanCredentials snowmanCredentials;
  private static Path testDir;
  private static String realm;
  private static PolarisTestClient userClient;

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken userToken,
      SnowmanCredentialsExtension.SnowmanCredentials snowmanCredentials,
      @PolarisRealm String polarisRealm)
      throws IOException {
    realm = polarisRealm;

    testDir = Path.of("build/test_data/iceberg/" + realm);
    FileUtils.deleteQuietly(testDir.toFile());
    Files.createDirectories(testDir);
    PolarisApplicationIntegrationTest.userToken = userToken.token();
    PolarisApplicationIntegrationTest.snowmanCredentials = snowmanCredentials;

    userClient = new PolarisTestClient(EXT.client(), EXT.getLocalPort(), userToken.token(), realm);
    PrincipalRole principalRole = new PrincipalRole(PRINCIPAL_ROLE_NAME);
    try (Response createPrResponse = userClient.createPrincipalRole(principalRole)) {
      assertThat(createPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    try (Response assignPrResponse = userClient.assignPrincipalRole("snowman", principalRole)) {
      assertThat(assignPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  @AfterAll
  public static void deletePrincipalRole() {
    userClient.deletePrincipalRole(PRINCIPAL_ROLE_NAME).close();
  }

  /**
   * Create a new catalog for each test case. Assign the snowman catalog-admin principal role the
   * admin role of the new catalog.
   *
   * @param testInfo
   */
  @BeforeEach
  public void before(TestInfo testInfo) {
    testInfo
        .getTestMethod()
        .ifPresent(
            method -> {
              String catalogName = method.getName();
              Catalog.TypeEnum catalogType = Catalog.TypeEnum.INTERNAL;
              createCatalog(catalogName, catalogType, PRINCIPAL_ROLE_NAME);
            });
  }

  private static void createCatalog(
      String catalogName, Catalog.TypeEnum catalogType, String principalRoleName) {
    createCatalog(
        catalogName,
        catalogType,
        principalRoleName,
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build(),
        "s3://my-bucket/path/to/data");
  }

  private static void createCatalog(
      String catalogName,
      Catalog.TypeEnum catalogType,
      String principalRoleName,
      StorageConfigInfo storageConfig,
      String defaultBaseLocation) {
    CatalogProperties props =
        CatalogProperties.builder(defaultBaseLocation)
            .addProperty(
                CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, "file:/")
            .build();
    Catalog catalog =
        catalogType.equals(Catalog.TypeEnum.INTERNAL)
            ? PolarisCatalog.builder()
                .setName(catalogName)
                .setType(catalogType)
                .setProperties(props)
                .setStorageConfigInfo(storageConfig)
                .build()
            : ExternalCatalog.builder()
                .setRemoteUrl("http://faraway.com")
                .setName(catalogName)
                .setType(catalogType)
                .setProperties(props)
                .setStorageConfigInfo(storageConfig)
                .build();
    try (Response response = userClient.createCatalog(catalog)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response =
        userClient.getCatalogRole(
            catalogName, PolarisEntityConstants.getNameOfCatalogAdminRole())) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      CatalogRole catalogRole = response.readEntity(CatalogRole.class);

      try (Response assignResponse =
          userClient.grantCatalogRoleToPrincipalRole(principalRoleName, catalogName, catalogRole)) {
        assertThat(assignResponse)
            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      }
    }
  }

  private static RESTSessionCatalog newSessionCatalog(String catalog) {
    RESTSessionCatalog sessionCatalog = new RESTSessionCatalog();
    sessionCatalog.initialize(
        "polaris_catalog_test",
        Map.of(
            "uri",
            "http://localhost:" + EXT.getLocalPort() + "/api/catalog",
            OAuth2Properties.CREDENTIAL,
            snowmanCredentials.clientId() + ":" + snowmanCredentials.clientSecret(),
            OAuth2Properties.SCOPE,
            BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL,
            "warehouse",
            catalog,
            "header." + REALM_PROPERTY_KEY,
            realm));
    return sessionCatalog;
  }

  @Test
  public void testIcebergListNamespaces() throws IOException {
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog("testIcebergListNamespaces")) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      List<Namespace> namespaces = sessionCatalog.listNamespaces(sessionContext);
      assertThat(namespaces).isNotNull().isEmpty();
    }
  }

  @Test
  public void testConfigureCatalogCaseSensitive() throws IOException {
    assertThatThrownBy(() -> newSessionCatalog("TESTCONFIGURECATALOGCASESENSITIVE"))
        .isInstanceOf(RESTException.class)
        .hasMessage(
            "Unable to process: Unable to find warehouse TESTCONFIGURECATALOGCASESENSITIVE");
  }

  @Test
  public void testIcebergListNamespacesNotFound() throws IOException {
    try (RESTSessionCatalog sessionCatalog =
        newSessionCatalog("testIcebergListNamespacesNotFound")) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      assertThatThrownBy(
              () -> sessionCatalog.listNamespaces(sessionContext, Namespace.of("whoops")))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessage("Namespace does not exist: whoops");
    }
  }

  @Test
  public void testIcebergListNamespacesNestedNotFound() throws IOException {
    try (RESTSessionCatalog sessionCatalog =
        newSessionCatalog("testIcebergListNamespacesNestedNotFound")) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace topLevelNamespace = Namespace.of("top_level");
      sessionCatalog.createNamespace(sessionContext, topLevelNamespace);
      sessionCatalog.loadNamespaceMetadata(sessionContext, Namespace.of("top_level"));
      assertThatThrownBy(
              () ->
                  sessionCatalog.listNamespaces(
                      sessionContext, Namespace.of("top_level", "whoops")))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessage("Namespace does not exist: top_level.whoops");
    }
  }

  @Test
  public void testIcebergListTablesNamespaceNotFound() throws IOException {
    try (RESTSessionCatalog sessionCatalog =
        newSessionCatalog("testIcebergListTablesNamespaceNotFound")) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      assertThatThrownBy(() -> sessionCatalog.listTables(sessionContext, Namespace.of("whoops")))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessage("Namespace does not exist: whoops");
    }
  }

  @Test
  public void testIcebergCreateNamespace() throws IOException {
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog("testIcebergCreateNamespace")) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace topLevelNamespace = Namespace.of("top_level");
      sessionCatalog.createNamespace(sessionContext, topLevelNamespace);
      List<Namespace> namespaces = sessionCatalog.listNamespaces(sessionContext);
      assertThat(namespaces).isNotNull().hasSize(1).containsExactly(topLevelNamespace);
      Namespace nestedNamespace = Namespace.of("top_level", "second_level");
      sessionCatalog.createNamespace(sessionContext, nestedNamespace);
      namespaces = sessionCatalog.listNamespaces(sessionContext, topLevelNamespace);
      assertThat(namespaces).isNotNull().hasSize(1).containsExactly(nestedNamespace);
    }
  }

  @Test
  public void testIcebergCreateNamespaceInExternalCatalog(TestInfo testInfo) throws IOException {
    String catalogName = testInfo.getTestMethod().get().getName() + "External";
    createCatalog(catalogName, Catalog.TypeEnum.EXTERNAL, PRINCIPAL_ROLE_NAME);
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName)) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      List<Namespace> namespaces = sessionCatalog.listNamespaces(sessionContext);
      assertThat(namespaces).isNotNull().hasSize(1).containsExactly(ns);
      Map<String, String> metadata = sessionCatalog.loadNamespaceMetadata(sessionContext, ns);
      assertThat(metadata)
          .isNotNull()
          .isNotEmpty()
          .containsEntry(
              PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://my-bucket/path/to/data/db1");
    }
  }

  @Test
  public void testIcebergDropNamespaceInExternalCatalog(TestInfo testInfo) throws IOException {
    String catalogName = testInfo.getTestMethod().get().getName() + "External";
    createCatalog(catalogName, Catalog.TypeEnum.EXTERNAL, PRINCIPAL_ROLE_NAME);
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName)) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      List<Namespace> namespaces = sessionCatalog.listNamespaces(sessionContext);
      assertThat(namespaces).isNotNull().hasSize(1).containsExactly(ns);
      sessionCatalog.dropNamespace(sessionContext, ns);
      assertThatThrownBy(() -> sessionCatalog.loadNamespaceMetadata(sessionContext, ns))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessage("Namespace does not exist: db1");
    }
  }

  @Test
  public void testIcebergCreateTablesInExternalCatalog(TestInfo testInfo) throws IOException {
    String catalogName = testInfo.getTestMethod().get().getName() + "External";
    createCatalog(catalogName, Catalog.TypeEnum.EXTERNAL, PRINCIPAL_ROLE_NAME);
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName)) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      assertThatThrownBy(
              () ->
                  sessionCatalog
                      .buildTable(
                          sessionContext,
                          TableIdentifier.of(ns, "the_table"),
                          new Schema(
                              List.of(
                                  Types.NestedField.of(
                                      1, false, "theField", Types.StringType.get()))))
                      .withLocation("file:///tmp/tables")
                      .withSortOrder(SortOrder.unsorted())
                      .withPartitionSpec(PartitionSpec.unpartitioned())
                      .create())
          .isInstanceOf(BadRequestException.class)
          .hasMessage("Malformed request: Cannot create table on external catalogs.");
    }
  }

  @Test
  public void testIcebergCreateTablesWithWritePathBlocked(TestInfo testInfo) throws IOException {
    String catalogName = testInfo.getTestMethod().get().getName() + "Internal";
    createCatalog(catalogName, Catalog.TypeEnum.INTERNAL, PRINCIPAL_ROLE_NAME);
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName)) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      try {
        Assertions.assertThatThrownBy(
                () ->
                    sessionCatalog
                        .buildTable(
                            sessionContext,
                            TableIdentifier.of(ns, "the_table"),
                            new Schema(
                                List.of(
                                    Types.NestedField.of(
                                        1, false, "theField", Types.StringType.get()))))
                        .withSortOrder(SortOrder.unsorted())
                        .withPartitionSpec(PartitionSpec.unpartitioned())
                        .withProperties(Map.of("write.data.path", "s3://my-bucket/path/to/data"))
                        .create())
            .isInstanceOf(ForbiddenException.class)
            .hasMessageContaining("Forbidden: Invalid locations");

        Assertions.assertThatThrownBy(
                () ->
                    sessionCatalog
                        .buildTable(
                            sessionContext,
                            TableIdentifier.of(ns, "the_table"),
                            new Schema(
                                List.of(
                                    Types.NestedField.of(
                                        1, false, "theField", Types.StringType.get()))))
                        .withSortOrder(SortOrder.unsorted())
                        .withPartitionSpec(PartitionSpec.unpartitioned())
                        .withProperties(
                            Map.of("write.metadata.path", "s3://my-bucket/path/to/data"))
                        .create())
            .isInstanceOf(ForbiddenException.class)
            .hasMessageContaining("Forbidden: Invalid locations");
      } catch (BadRequestException e) {
        LOGGER.info("Received expected exception {}", e.getMessage());
      }
    }
  }

  @Test
  public void testIcebergRegisterTableInExternalCatalog(TestInfo testInfo) throws IOException {
    String catalogName = testInfo.getTestMethod().get().getName() + "External";
    createCatalog(
        catalogName,
        Catalog.TypeEnum.EXTERNAL,
        PRINCIPAL_ROLE_NAME,
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://" + testDir.toFile().getAbsolutePath()))
            .build(),
        "file://" + testDir.toFile().getAbsolutePath());
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName);
        HadoopFileIO fileIo = new HadoopFileIO(new Configuration()); ) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      TableIdentifier tableIdentifier = TableIdentifier.of(ns, "the_table");
      String location =
          "file://"
              + testDir.toFile().getAbsolutePath()
              + "/"
              + testInfo.getTestMethod().get().getName();
      String metadataLocation = location + "/metadata/000001-494949494949494949.metadata.json";

      TableMetadata tableMetadata =
          TableMetadata.buildFromEmpty()
              .setLocation(location)
              .assignUUID()
              .addPartitionSpec(PartitionSpec.unpartitioned())
              .addSortOrder(SortOrder.unsorted())
              .addSchema(
                  new Schema(Types.NestedField.of(1, false, "col1", Types.StringType.get())), 1)
              .build();
      TableMetadataParser.write(tableMetadata, fileIo.newOutputFile(metadataLocation));

      sessionCatalog.registerTable(sessionContext, tableIdentifier, metadataLocation);
      Table table = sessionCatalog.loadTable(sessionContext, tableIdentifier);
      assertThat(table)
          .isNotNull()
          .isInstanceOf(BaseTable.class)
          .asInstanceOf(InstanceOfAssertFactories.type(BaseTable.class))
          .returns(tableMetadata.location(), BaseTable::location)
          .returns(tableMetadata.uuid(), bt -> bt.uuid().toString())
          .returns(tableMetadata.schema().columns(), bt -> bt.schema().columns());
    }
  }

  @Test
  public void testIcebergUpdateTableInExternalCatalog(TestInfo testInfo) throws IOException {
    String catalogName = testInfo.getTestMethod().get().getName() + "External";
    createCatalog(
        catalogName,
        Catalog.TypeEnum.EXTERNAL,
        PRINCIPAL_ROLE_NAME,
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://" + testDir.toFile().getAbsolutePath()))
            .build(),
        "file://" + testDir.toFile().getAbsolutePath());
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName);
        HadoopFileIO fileIo = new HadoopFileIO(new Configuration()); ) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      TableIdentifier tableIdentifier = TableIdentifier.of(ns, "the_table");
      String location =
          "file://"
              + testDir.toFile().getAbsolutePath()
              + "/"
              + testInfo.getTestMethod().get().getName();
      String metadataLocation = location + "/metadata/000001-494949494949494949.metadata.json";

      Types.NestedField col1 = Types.NestedField.of(1, false, "col1", Types.StringType.get());
      TableMetadata tableMetadata =
          TableMetadata.buildFromEmpty()
              .setLocation(location)
              .assignUUID()
              .addPartitionSpec(PartitionSpec.unpartitioned())
              .addSortOrder(SortOrder.unsorted())
              .addSchema(new Schema(col1), 1)
              .build();
      TableMetadataParser.write(tableMetadata, fileIo.newOutputFile(metadataLocation));

      sessionCatalog.registerTable(sessionContext, tableIdentifier, metadataLocation);
      Table table = sessionCatalog.loadTable(sessionContext, tableIdentifier);
      ((ResolvingFileIO) table.io()).setConf(new Configuration());
      assertThatThrownBy(
              () ->
                  table
                      .newAppend()
                      .appendFile(
                          new TestHelpers.TestDataFile(
                              location + "/path/to/file.parquet",
                              new PartitionData(PartitionSpec.unpartitioned().partitionType()),
                              10L))
                      .commit())
          .isInstanceOf(BadRequestException.class)
          .hasMessage("Malformed request: Cannot update table on external catalogs.");
    }
  }

  @Test
  public void testIcebergDropTableInExternalCatalog(TestInfo testInfo) throws IOException {
    String catalogName = testInfo.getTestMethod().get().getName() + "External";
    createCatalog(
        catalogName,
        Catalog.TypeEnum.EXTERNAL,
        PRINCIPAL_ROLE_NAME,
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://" + testDir.toFile().getAbsolutePath()))
            .build(),
        "file://" + testDir.toFile().getAbsolutePath());
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName);
        HadoopFileIO fileIo = new HadoopFileIO(new Configuration()); ) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      TableIdentifier tableIdentifier = TableIdentifier.of(ns, "the_table");
      String location =
          "file://"
              + testDir.toFile().getAbsolutePath()
              + "/"
              + testInfo.getTestMethod().get().getName();
      String metadataLocation = location + "/metadata/000001-494949494949494949.metadata.json";

      TableMetadata tableMetadata =
          TableMetadata.buildFromEmpty()
              .setLocation(location)
              .assignUUID()
              .addPartitionSpec(PartitionSpec.unpartitioned())
              .addSortOrder(SortOrder.unsorted())
              .addSchema(
                  new Schema(Types.NestedField.of(1, false, "col1", Types.StringType.get())), 1)
              .build();
      TableMetadataParser.write(tableMetadata, fileIo.newOutputFile(metadataLocation));

      sessionCatalog.registerTable(sessionContext, tableIdentifier, metadataLocation);
      Table table = sessionCatalog.loadTable(sessionContext, tableIdentifier);
      assertThat(table).isNotNull();
      sessionCatalog.dropTable(sessionContext, tableIdentifier);
      assertThatThrownBy(() -> sessionCatalog.loadTable(sessionContext, tableIdentifier))
          .isInstanceOf(NoSuchTableException.class)
          .hasMessage("Table does not exist: db1.the_table");
    }
  }

  @Test
  public void testWarehouseNotSpecified() throws IOException {
    try (RESTSessionCatalog sessionCatalog = new RESTSessionCatalog()) {
      String emptyEnvironmentVariable = "env:__NULL_ENV_VARIABLE__";
      assertThat(EnvironmentUtil.resolveAll(Map.of("", emptyEnvironmentVariable)).get("")).isNull();
      assertThatThrownBy(
              () ->
                  sessionCatalog.initialize(
                      "polaris_catalog_test",
                      Map.of(
                          "uri",
                          "http://localhost:" + EXT.getLocalPort() + "/api/catalog",
                          OAuth2Properties.CREDENTIAL,
                          snowmanCredentials.clientId() + ":" + snowmanCredentials.clientSecret(),
                          OAuth2Properties.SCOPE,
                          BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL,
                          "warehouse",
                          emptyEnvironmentVariable,
                          "header." + REALM_PROPERTY_KEY,
                          realm)))
          .isInstanceOf(BadRequestException.class)
          .hasMessage("Malformed request: Please specify a warehouse");
    }
  }

  @Test
  public void testRequestHeaderTooLarge() {
    Invocation.Builder request =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/principal-roles", EXT.getLocalPort()))
            .request("application/json");

    // The default limit is 8KiB and each of these headers is at least 8 bytes, so 1500 definitely
    // exceeds the limit
    for (int i = 0; i < 1500; i++) {
      request = request.header("header" + i, "" + i);
    }

    try {
      try (Response response =
          request
              .header("Authorization", "Bearer " + userToken)
              .header(REALM_PROPERTY_KEY, realm)
              .post(Entity.json(new PrincipalRole("r")))) {
        assertThat(response)
            .returns(
                Response.Status.REQUEST_HEADER_FIELDS_TOO_LARGE.getStatusCode(),
                Response::getStatus);
      }
    } catch (ProcessingException e) {
      // In some runtime environments the request above will return a 431 but in others it'll result
      // in a ProcessingException from the socket being closed. The test asserts that one of those
      // things happens.
    }
  }

  @Test
  public void testRequestBodyTooLarge() {
    // The size is set to be higher than the limit in polaris-server-integrationtest.yml
    Entity<PrincipalRole> largeRequest = Entity.json(new PrincipalRole("r".repeat(1000001)));

    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/principal-roles", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .post(largeRequest)) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus)
          .matches(
              r ->
                  r.readEntity(RequestThrottlingErrorResponse.class)
                      .errorType()
                      .equals(REQUEST_TOO_LARGE));
    }
  }

  @Test
  public void testRefreshToken() throws IOException {
    String path =
        String.format("http://localhost:%d/api/catalog/v1/oauth/tokens", EXT.getLocalPort());
    try (RESTClient client =
        HTTPClient.builder(ImmutableMap.of())
            .withHeader(REALM_PROPERTY_KEY, realm)
            .uri(path)
            .build()) {
      String credentialString =
          snowmanCredentials.clientId() + ":" + snowmanCredentials.clientSecret();
      var authConfig =
          AuthConfig.builder().credential(credentialString).scope("PRINCIPAL_ROLE:ALL").build();
      ImmutableAuthConfig configSpy = spy(authConfig);
      when(configSpy.expiresAtMillis()).thenReturn(0L);
      assertThat(configSpy.expiresAtMillis()).isEqualTo(0L);
      when(configSpy.oauth2ServerUri()).thenReturn(path);

      var parentSession = new OAuth2Util.AuthSession(Map.of(), configSpy);
      var session =
          OAuth2Util.AuthSession.fromAccessToken(client, null, userToken, 0L, parentSession);

      OAuth2Util.AuthSession sessionSpy = spy(session);
      when(sessionSpy.expiresAtMillis()).thenReturn(0L);
      assertThat(sessionSpy.expiresAtMillis()).isEqualTo(0L);
      assertThat(sessionSpy.token()).isEqualTo(userToken);

      sessionSpy.refresh(client);
      assertThat(sessionSpy.credential()).isNotNull();
      assertThat(sessionSpy.credential()).isNotEqualTo(userToken);
    }
  }
}
