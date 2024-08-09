/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service;

import static io.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.polaris.core.admin.model.AwsStorageConfigInfo;
import io.polaris.core.admin.model.Catalog;
import io.polaris.core.admin.model.CatalogProperties;
import io.polaris.core.admin.model.CatalogRole;
import io.polaris.core.admin.model.ExternalCatalog;
import io.polaris.core.admin.model.FileStorageConfigInfo;
import io.polaris.core.admin.model.PolarisCatalog;
import io.polaris.core.admin.model.PrincipalRole;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.core.entity.CatalogEntity;
import io.polaris.core.entity.PolarisEntityConstants;
import io.polaris.service.auth.BasePolarisAuthenticator;
import io.polaris.service.config.PolarisApplicationConfig;
import io.polaris.service.test.PolarisConnectionExtension;
import io.polaris.service.test.SnowmanCredentialsExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.EnvironmentUtil;
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

@ExtendWith({
  DropwizardExtensionsSupport.class,
  PolarisConnectionExtension.class,
  SnowmanCredentialsExtension.class
})
public class PolarisApplicationIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisApplicationIntegrationTest.class);

  public static final String PRINCIPAL_ROLE_NAME = "admin";
  private static DropwizardAppExtension<PolarisApplicationConfig> EXT =
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

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken userToken,
      SnowmanCredentialsExtension.SnowmanCredentials snowmanCredentials)
      throws IOException {
    realm = PolarisConnectionExtension.getTestRealm(PolarisApplicationIntegrationTest.class);

    testDir = Path.of("build/test_data/iceberg/" + realm);
    if (Files.exists(testDir)) {
      if (Files.isDirectory(testDir)) {
        Files.walk(testDir)
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });

      } else {
        Files.delete(testDir);
      }
    }
    Files.createDirectories(testDir);
    PolarisApplicationIntegrationTest.userToken = userToken.token();
    PolarisApplicationIntegrationTest.snowmanCredentials = snowmanCredentials;

    PrincipalRole principalRole = new PrincipalRole(PRINCIPAL_ROLE_NAME);
    try (Response createPrResponse =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/principal-roles", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(principalRole))) {
      assertThat(createPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    try (Response assignPrResponse =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/principals/snowman/principal-roles",
                    EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "Bearer " + PolarisApplicationIntegrationTest.userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .put(Entity.json(principalRole))) {
      assertThat(assignPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  @AfterAll
  public static void deletePrincipalRole() {
    try (Response deletePrResponse =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/principal-roles/%s",
                    EXT.getLocalPort(), PRINCIPAL_ROLE_NAME))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .delete()) {}
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
    try (Response response =
        EXT.client()
            .target(
                String.format("http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/%s",
                    EXT.getLocalPort(),
                    catalogName,
                    PolarisEntityConstants.getNameOfCatalogAdminRole()))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      CatalogRole catalogRole = response.readEntity(CatalogRole.class);

      try (Response assignResponse =
          EXT.client()
              .target(
                  String.format(
                      "http://localhost:%d/api/management/v1/principal-roles/%s/catalog-roles/%s",
                      EXT.getLocalPort(), principalRoleName, catalogName))
              .request("application/json")
              .header("Authorization", "Bearer " + userToken)
              .header(REALM_PROPERTY_KEY, realm)
              .put(Entity.json(catalogRole))) {
        assertThat(assignResponse)
            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      }
    }
  }

  private static RESTSessionCatalog newSessionCatalog(String catalog) {
    RESTSessionCatalog sessionCatalog = new RESTSessionCatalog();
    sessionCatalog.initialize(
        "snowflake",
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
    try {
      RESTSessionCatalog sessionCatalog = newSessionCatalog("TESTCONFIGURECATALOGCASESENSITIVE");
      fail("Expected exception connecting to catalog");
    } catch (ServiceFailureException e) {
      fail("Unexpected service failure exception", e);
    } catch (RESTException e) {
      LoggerFactory.getLogger(getClass()).info("Caught expected rest exception", e);
    }
  }

  @Test
  public void testIcebergListNamespacesNotFound() throws IOException {
    try (RESTSessionCatalog sessionCatalog =
        newSessionCatalog("testIcebergListNamespacesNotFound")) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      assertThatThrownBy(
              () -> sessionCatalog.listNamespaces(sessionContext, Namespace.of("whoops")))
          .isInstanceOf(NoSuchNamespaceException.class);
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
          .isInstanceOf(NoSuchNamespaceException.class);
    }
  }

  @Test
  public void testIcebergListTablesNamespaceNotFound() throws IOException {
    try (RESTSessionCatalog sessionCatalog =
        newSessionCatalog("testIcebergListTablesNamespaceNotFound")) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      assertThatThrownBy(() -> sessionCatalog.listTables(sessionContext, Namespace.of("whoops")))
          .isInstanceOf(NoSuchNamespaceException.class);
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
      try {
        sessionCatalog.loadNamespaceMetadata(sessionContext, ns);
        Assertions.fail("Expected exception when loading namespace after drop");
      } catch (NoSuchNamespaceException e) {
        LOGGER.info("Received expected exception {}", e.getMessage());
      }
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
      try {
        sessionCatalog
            .buildTable(
                sessionContext,
                TableIdentifier.of(ns, "the_table"),
                new Schema(
                    List.of(Types.NestedField.of(1, false, "theField", Types.StringType.get()))))
            .withLocation("file:///tmp/tables")
            .withSortOrder(SortOrder.unsorted())
            .withPartitionSpec(PartitionSpec.unpartitioned())
            .create();
        Assertions.fail("Expected failure calling create table in external catalog");
      } catch (BadRequestException e) {
        LOGGER.info("Received expected exception {}", e.getMessage());
      }
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
            .hasMessage(
                "Forbidden: Delegate access to table with user-specified write location is temporarily not supported.");

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
            .hasMessage(
                "Forbidden: Delegate access to table with user-specified write location is temporarily not supported.");
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
      try {
        table
            .newAppend()
            .appendFile(
                new TestHelpers.TestDataFile(
                    location + "/path/to/file.parquet",
                    new PartitionData(PartitionSpec.unpartitioned().partitionType()),
                    10L))
            .commit();
        Assertions.fail("Should fail when committing an update to external catalog");
      } catch (BadRequestException e) {
        LOGGER.info("Received expected exception {}", e.getMessage());
      }
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
      try {
        sessionCatalog.loadTable(sessionContext, tableIdentifier);
        Assertions.fail("Expected failure loading table after drop");
      } catch (NoSuchTableException e) {
        LOGGER.info("Received expected exception {}", e.getMessage());
      }
    }
  }

  @Test
  public void testWarehouseNotSpecified() throws IOException {
    try (RESTSessionCatalog sessionCatalog = new RESTSessionCatalog()) {
      String emptyEnvironmentVariable = "env:__NULL_ENV_VARIABLE__";
      assertThat(EnvironmentUtil.resolveAll(Map.of("", emptyEnvironmentVariable)).get("")).isNull();
      sessionCatalog.initialize(
          "snowflake",
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
              realm));
      fail("Expected exception due to null warehouse");
    } catch (ServiceFailureException e) {
      fail("Unexpected service failure exception", e);
    } catch (RESTException e) {
      LoggerFactory.getLogger(getClass()).info("Caught expected rest exception", e);
      assertThat(e).isInstanceOf(BadRequestException.class);
    }
  }
}
