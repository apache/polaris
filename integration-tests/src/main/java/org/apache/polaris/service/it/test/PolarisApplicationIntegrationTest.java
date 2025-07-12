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

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
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
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.RestApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * @implSpec This test expects the server to be configured with the following features configured:
 *     <ul>
 *       <li>{@link
 *           org.apache.polaris.core.config.FeatureConfiguration#ALLOW_OVERLAPPING_CATALOG_URLS}:
 *           {@code true}
 *       <li>{@link
 *           org.apache.polaris.core.config.FeatureConfiguration#SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION}:
 *           {@code true}
 *     </ul>
 *     The server must also be configured to reject request body sizes larger than 1MB (1000000
 *     bytes).
 *     <p>The server must also be configured with the following realms: POLARIS (default), and
 *     OTHER.
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisApplicationIntegrationTest {

  public static final String PRINCIPAL_ROLE_ALL = "PRINCIPAL_ROLE:ALL";

  private static String realm;

  private static RestApi managementApi;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ClientCredentials clientCredentials;
  private static ClientPrincipal admin;
  private static String authToken;
  private static URI baseLocation;

  private String principalRoleName;
  private String internalCatalogName;

  @BeforeAll
  public static void setup(
      PolarisApiEndpoints apiEndpoints, ClientPrincipal adminCredentials, @TempDir Path tempDir) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    realm = endpoints.realmId();
    admin = adminCredentials;
    clientCredentials = adminCredentials.credentials();
    authToken = client.obtainToken(clientCredentials);
    managementApi = client.managementApi(clientCredentials);
    baseLocation = IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(realm + "/");
  }

  @AfterAll
  public static void close() throws Exception {
    client.close();
  }

  /**
   * Create a new catalog for each test case. Assign the snowman catalog-admin principal role the
   * admin role of the new catalog.
   */
  @BeforeEach
  public void before(TestInfo testInfo) {
    principalRoleName = client.newEntityName("admin");
    PrincipalRole principalRole = new PrincipalRole(principalRoleName);
    try (Response createPrResponse =
        managementApi.request("v1/principal-roles").post(Entity.json(principalRole))) {
      assertThat(createPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    try (Response assignPrResponse =
        managementApi
            .request("v1/principals/{name}/principal-roles", Map.of("name", admin.principalName()))
            .put(Entity.json(principalRole))) {
      assertThat(assignPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    internalCatalogName = client.newEntityName(testInfo.getTestMethod().orElseThrow().getName());
    createCatalog(internalCatalogName, Catalog.TypeEnum.INTERNAL, principalRoleName);
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(clientCredentials);
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
                .setName(catalogName)
                .setType(catalogType)
                .setProperties(props)
                .setStorageConfigInfo(storageConfig)
                .build();
    try (Response response = managementApi.request("v1/catalogs").post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    try (Response response =
        managementApi
            .request(
                "v1/catalogs/{cat}/catalog-roles/{role}",
                Map.of(
                    "cat", catalogName, "role", PolarisEntityConstants.getNameOfCatalogAdminRole()))
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      CatalogRole catalogRole = response.readEntity(CatalogRole.class);

      try (Response assignResponse =
          managementApi
              .request(
                  "v1/principal-roles/{prin-role}/catalog-roles/{cat}",
                  Map.of("cat", catalogName, "prin-role", principalRoleName))
              .put(Entity.json(catalogRole))) {
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
            endpoints.catalogApiEndpoint().toString(),
            OAuth2Properties.TOKEN,
            authToken,
            "warehouse",
            catalog,
            "header." + endpoints.realmHeaderName(),
            realm));
    return sessionCatalog;
  }

  @Test
  public void testIcebergListNamespaces() throws IOException {
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(internalCatalogName)) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      List<Namespace> namespaces = sessionCatalog.listNamespaces(sessionContext);
      assertThat(namespaces).isNotNull().isEmpty();
    }
  }

  @Test
  public void testConfigureCatalogCaseSensitive() {
    assertThatThrownBy(() -> newSessionCatalog("TESTCONFIGURECATALOGCASESENSITIVE"))
        .isInstanceOf(RESTException.class)
        .hasMessage(
            "Unable to process: Unable to find warehouse TESTCONFIGURECATALOGCASESENSITIVE");
  }

  @Test
  public void testIcebergListNamespacesNotFound() throws IOException {
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(internalCatalogName)) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      assertThatThrownBy(
              () -> sessionCatalog.listNamespaces(sessionContext, Namespace.of("whoops")))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessage("Namespace does not exist: whoops");
    }
  }

  @Test
  public void testIcebergListNamespacesNestedNotFound() throws IOException {
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(internalCatalogName)) {
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
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(internalCatalogName)) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      assertThatThrownBy(() -> sessionCatalog.listTables(sessionContext, Namespace.of("whoops")))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessage("Namespace does not exist: whoops");
    }
  }

  @Test
  public void testIcebergCreateNamespace() throws IOException {
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(internalCatalogName)) {
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
  public void testIcebergCreateNamespaceInExternalCatalog() throws IOException {
    String catalogName =
        client.newEntityName("testIcebergCreateNamespaceInExternalCatalogExternal");
    createCatalog(catalogName, Catalog.TypeEnum.EXTERNAL, principalRoleName);
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
              PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://my-bucket/path/to/data/db1/");
    }
  }

  @Test
  public void testIcebergDropNamespaceInExternalCatalog() throws IOException {
    String catalogName = client.newEntityName("testIcebergDropNamespaceInExternalCatalogExternal");
    createCatalog(catalogName, Catalog.TypeEnum.EXTERNAL, principalRoleName);
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
  public void testIcebergCreateTablesInExternalCatalog() throws IOException {
    String catalogName = client.newEntityName("testIcebergCreateTablesInExternalCatalogExternal");
    createCatalog(catalogName, Catalog.TypeEnum.EXTERNAL, principalRoleName);
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
                                  Types.NestedField.required(
                                      1, "theField", Types.StringType.get()))))
                      .withLocation("file:///tmp/tables")
                      .withSortOrder(SortOrder.unsorted())
                      .withPartitionSpec(PartitionSpec.unpartitioned())
                      .create())
          .isInstanceOf(BadRequestException.class)
          .hasMessage("Malformed request: Cannot create table on static-facade external catalogs.");
    }
  }

  @Test
  public void testIcebergCreateTablesWithWritePathBlocked() throws IOException {
    String catalogName =
        client.newEntityName("testIcebergCreateTablesWithWritePathBlockedInternal");
    createCatalog(catalogName, Catalog.TypeEnum.INTERNAL, principalRoleName);
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
                                  Types.NestedField.required(
                                      1, "theField", Types.StringType.get()))))
                      .withSortOrder(SortOrder.unsorted())
                      .withPartitionSpec(PartitionSpec.unpartitioned())
                      .withProperties(Map.of("write.data.path", "s3://my-bucket/path/to/data"))
                      .create())
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Forbidden: Invalid locations");

      assertThatThrownBy(
              () ->
                  sessionCatalog
                      .buildTable(
                          sessionContext,
                          TableIdentifier.of(ns, "the_table"),
                          new Schema(
                              List.of(
                                  Types.NestedField.required(
                                      1, "theField", Types.StringType.get()))))
                      .withSortOrder(SortOrder.unsorted())
                      .withPartitionSpec(PartitionSpec.unpartitioned())
                      .withProperties(Map.of("write.metadata.path", "s3://my-bucket/path/to/data"))
                      .create())
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Forbidden: Invalid locations");
    }
  }

  @Test
  public void testIcebergRegisterTableInExternalCatalog() throws IOException {
    String catalogName = client.newEntityName("testIcebergRegisterTableInExternalCatalogExternal");
    createCatalog(
        catalogName,
        Catalog.TypeEnum.EXTERNAL,
        principalRoleName,
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(baseLocation.toString()))
            .build(),
        baseLocation.toString());
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName);
        HadoopFileIO fileIo = new HadoopFileIO(new Configuration())) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      TableIdentifier tableIdentifier = TableIdentifier.of(ns, "the_table");
      String location =
          baseLocation.resolve("testIcebergRegisterTableInExternalCatalog").toString();
      String metadataLocation = location + "/metadata/000001-494949494949494949.metadata.json";

      TableMetadata tableMetadata =
          TableMetadata.buildFromEmpty()
              .setLocation(location)
              .assignUUID()
              .addPartitionSpec(PartitionSpec.unpartitioned())
              .addSortOrder(SortOrder.unsorted())
              .addSchema(new Schema(Types.NestedField.required(1, "col1", Types.StringType.get())))
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
  public void testIcebergUpdateTableInExternalCatalog() throws IOException {
    String catalogName = client.newEntityName("testIcebergUpdateTableInExternalCatalogExternal");
    createCatalog(
        catalogName,
        Catalog.TypeEnum.EXTERNAL,
        principalRoleName,
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(baseLocation.toString()))
            .build(),
        baseLocation.toString());
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName);
        HadoopFileIO fileIo = new HadoopFileIO(new Configuration())) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      TableIdentifier tableIdentifier = TableIdentifier.of(ns, "the_table");
      String location = baseLocation.resolve("testIcebergUpdateTableInExternalCatalog").toString();
      String metadataLocation = location + "/metadata/000001-494949494949494949.metadata.json";

      Types.NestedField col1 = Types.NestedField.required(1, "col1", Types.StringType.get());
      TableMetadata tableMetadata =
          TableMetadata.buildFromEmpty()
              .setLocation(location)
              .assignUUID()
              .addPartitionSpec(PartitionSpec.unpartitioned())
              .addSortOrder(SortOrder.unsorted())
              .addSchema(new Schema(col1))
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
          .hasMessage("Malformed request: Cannot update table on static-facade external catalogs.");
    }
  }

  @Test
  public void testIcebergDropTableInExternalCatalog() throws IOException {
    String catalogName = client.newEntityName("testIcebergDropTableInExternalCatalogExternal");
    createCatalog(
        catalogName,
        Catalog.TypeEnum.EXTERNAL,
        principalRoleName,
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(baseLocation.toString()))
            .build(),
        baseLocation.toString());
    try (RESTSessionCatalog sessionCatalog = newSessionCatalog(catalogName);
        HadoopFileIO fileIo = new HadoopFileIO(new Configuration())) {
      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);
      TableIdentifier tableIdentifier = TableIdentifier.of(ns, "the_table");
      String location = baseLocation.resolve("testIcebergDropTableInExternalCatalog").toString();
      String metadataLocation = location + "/metadata/000001-494949494949494949.metadata.json";

      TableMetadata tableMetadata =
          TableMetadata.buildFromEmpty()
              .setLocation(location)
              .assignUUID()
              .addPartitionSpec(PartitionSpec.unpartitioned())
              .addSortOrder(SortOrder.unsorted())
              .addSchema(new Schema(Types.NestedField.required(1, "col1", Types.StringType.get())))
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
                          endpoints.catalogApiEndpoint().toString(),
                          OAuth2Properties.TOKEN,
                          authToken,
                          "warehouse",
                          emptyEnvironmentVariable,
                          "header." + endpoints.realmHeaderName(),
                          realm)))
          .isInstanceOf(BadRequestException.class)
          .hasMessage("Malformed request: Please specify a warehouse");
    }
  }

  @Test
  public void testRequestHeaderTooLarge() throws Exception {
    // Use a dedicated client with retries due to non-deterministic behaviour of reading the
    // response code and possible abrupt connection resets
    try (PolarisClient localClient = polarisClient(endpoints)) {
      await()
          .atMost(Duration.of(1, ChronoUnit.MINUTES))
          .untilAsserted(
              () -> {
                Invocation.Builder request =
                    localClient.managementApi(clientCredentials).request("v1/principal-roles");
                // The default limit is 8KiB and each of these headers is at least 8 bytes, so 1500
                // definitely exceeds the limit
                for (int i = 0; i < 1500; i++) {
                  request = request.header("header" + i, "" + i);
                }

                try (Response response = request.post(Entity.json(new PrincipalRole("r")))) {
                  assertThat(response.getStatus())
                      .isEqualTo(Response.Status.REQUEST_HEADER_FIELDS_TOO_LARGE.getStatusCode());
                } catch (ProcessingException e) {
                  // In some runtime environments the request above will return a 431 but in others
                  // it'll result in a ProcessingException from the socket being closed. The test
                  // asserts that one of those things happens.
                }
              });
    }
  }

  @Test
  public void testRequestBodyTooLarge() throws Exception {
    // Use a dedicated client with retries due to non-deterministic behaviour of reading the
    // response code and possible abrupt connection resets
    try (PolarisClient localClient = polarisClient(endpoints)) {
      await()
          .atMost(Duration.of(1, ChronoUnit.MINUTES))
          .untilAsserted(
              () -> {
                // The behaviour in case of large requests depends on the specific server
                // configuration. This test assumes that the server under test is configured to deny
                // requests larger than 1000000 bytes. The test payload below assumes UTF8 encoding
                // of ASCII charts plus a bit of JSON overhead.
                Entity<PrincipalRole> largeRequest =
                    Entity.json(new PrincipalRole("r".repeat(1000001)));
                try (Response response =
                    localClient
                        .managementApi(clientCredentials)
                        .request("v1/principal-roles")
                        .post(largeRequest)) {
                  // Note we only validate the status code here because per RFC 9110, the server MAY
                  // not provide a response body. The HTTP status line is still expected to be
                  // provided most of the time.
                  assertThat(response.getStatus())
                      .isEqualTo(Response.Status.REQUEST_ENTITY_TOO_LARGE.getStatusCode());
                } catch (ProcessingException e) {
                  // In some runtime environments the request above will return a 431 but in others
                  // it'll result in a ProcessingException from the socket being closed. The test
                  // asserts that one of those things happens.
                }
              });
    }
  }
}
