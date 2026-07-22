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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.GcpAuthenticationParameters;
import org.apache.polaris.core.admin.model.GcpStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * You need to run your Polaris with: polaris.features."ENABLE_CATALOG_FEDERATION"=true for this
 * test to run against it successfully.
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
@EnabledIfEnvironmentVariable(named = "INTEGRATION_TEST_GCS_FEDERATED_PATH", matches = ".+")
public class GcpCatalogFederationIntegrationIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GcpCatalogFederationIntegrationIT.class);
  private static final String BIG_LAKE_REST_URI =
      "https://biglake.googleapis.com/iceberg/v1/restcatalog";
  private static final String PRESERVE_RESOURCES_ENV =
      "INTEGRATION_TEST_GCS_FEDERATED_PRESERVE_RESOURCES";
  private static final String REFRESH_WAIT_SECONDS_ENV =
      "INTEGRATION_TEST_GCS_FEDERATED_REFRESH_WAIT_SECONDS";
  private static final String OAUTH_SCOPE = "PRINCIPAL_ROLE:ALL";

  private static final String GOOGLE_APPLICATION_CREDENTIALS =
      "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String SERVICE_ACCOUNT_ENV = "INTEGRATION_TEST_GCS_SERVICE_ACCOUNT";
  private static final String FEDERATED_CATALOG_ENV = "INTEGRATION_TEST_GCS_FEDERATED_CATALOG";
  private static final String QUOTA_PROJECT_ENV = "INTEGRATION_TEST_GCS_FEDERATED_QUOTA_PROJECT";
  private static final String BASE_LOCATION_ENV = "INTEGRATION_TEST_GCS_FEDERATED_PATH";

  private static final String SERVICE_ACCOUNT = System.getenv(SERVICE_ACCOUNT_ENV);
  private static final String FEDERATED_CATALOG = System.getenv(FEDERATED_CATALOG_ENV);
  private static final String QUOTA_PROJECT = System.getenv(QUOTA_PROJECT_ENV);
  private static final String BASE_LOCATION = System.getenv(BASE_LOCATION_ENV);

  private static final CatalogGrant DEFAULT_CATALOG_GRANT =
      CatalogGrant.builder()
          .setType(GrantResource.TypeEnum.CATALOG)
          .setPrivilege(CatalogPrivilege.CATALOG_MANAGE_CONTENT)
          .build();

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "doc"),
          optional(2, "data", Types.StringType.get()));

  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;

  private final List<TableIdentifier> createdTables = new ArrayList<>();
  private final List<Namespace> createdNamespaces = new ArrayList<>();

  private PrincipalWithCredentials userCredentials;
  private String principalName;
  private String principalRoleName;
  private String localCatalogName;

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    validateCloudConfiguration();
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    String adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
  }

  @AfterAll
  static void closeClient() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @BeforeEach
  void before() {
    principalName = ownedEntityName("biglake_user");
    principalRoleName = ownedEntityName("biglake_user_role");
    localCatalogName = ownedEntityName("biglake_catalog");

    LOGGER.info(
        "Using federated catalog {}, local catalog {}, base location {}, quota project {}",
        FEDERATED_CATALOG,
        localCatalogName,
        BASE_LOCATION,
        QUOTA_PROJECT);

    userCredentials = managementApi.createPrincipalWithRole(principalName, principalRoleName);
    createCatalog();
    permissionCatalog();
  }

  @AfterEach
  void tearDown() {
    if (preserveResources()) {
      LOGGER.info(
          "Preserving BigLake test resources because {} is enabled. catalog={}, principal={}, role={}",
          PRESERVE_RESOURCES_ENV,
          localCatalogName,
          principalName,
          principalRoleName);
      return;
    }

    cleanupTablesAndNamespaces();
    bestEffort("drop local catalog", () -> managementApi.dropCatalog(localCatalogName));
    bestEffort("delete principal role", () -> managementApi.deletePrincipalRole(principalRoleName));
    bestEffort("delete principal", () -> managementApi.deletePrincipal(principalName));
  }

  @Test
  void testFederatedCatalogNamespaceTableAndDataWorkflows() throws IOException {
    String userToken = client.obtainToken(userCredentials);
    RESTCatalog sessionCatalog = newRestCatalogWithToken(userToken);
    CatalogApi userCatalogApi = client.catalogApi(userToken);

    Namespace namespace = createTrackedNamespace();
    TableIdentifier tableIdentifier = createTrackedTableIdentifier(namespace);
    String payload = "biglake-payload-" + UUID.randomUUID();

    sessionCatalog.createNamespace(namespace);
    assertThat(sessionCatalog.listNamespaces(Namespace.empty())).contains(namespace);

    Table createdTable = sessionCatalog.createTable(tableIdentifier, SCHEMA);
    createdTables.add(tableIdentifier);

    assertThat(createdTable).isNotNull();
    assertThat(createdTable.currentSnapshot()).isNull();
    assertThat(sessionCatalog.listTables(namespace)).contains(tableIdentifier);

    Table firstLoadedTable = sessionCatalog.loadTable(tableIdentifier);
    Table secondLoadedTable = sessionCatalog.loadTable(tableIdentifier);
    assertThat(firstLoadedTable.location()).isEqualTo(createdTable.location());
    assertThat(secondLoadedTable.location()).isEqualTo(createdTable.location());
    assertThat(secondLoadedTable.currentSnapshot()).isNull();

    LoadTableResponse loadTableResponse =
        userCatalogApi.loadTable(localCatalogName, tableIdentifier, "all");
    assertThat(loadTableResponse.tableMetadata()).isNotNull();
    assertThat(loadTableResponse.tableMetadata().location()).isEqualTo(createdTable.location());

    LoadTableResponse delegatedLoadResponse =
        userCatalogApi.loadTableWithAccessDelegation(localCatalogName, tableIdentifier, "all");
    assertThat(delegatedLoadResponse.credentials()).isNotEmpty();
    assertThat(delegatedLoadResponse.tableMetadata()).isNotNull();

    LoadCredentialsResponse credentialsResponse =
        userCatalogApi.loadCredentials(localCatalogName, tableIdentifier);
    assertThat(credentialsResponse.credentials()).isNotEmpty();

    writePayloadTo(createdTable, payload);

    assertThat(secondLoadedTable.currentSnapshot()).isNull();
    secondLoadedTable.refresh();
    assertThat(rowCountFor(secondLoadedTable)).isEqualTo(1L);
    assertThat(readPayloads(secondLoadedTable)).containsExactly(payload);

    assertThat(sessionCatalog.loadTable(tableIdentifier).location()).isEqualTo(createdTable.location());
    assertThat(sessionCatalog.listTables(namespace)).containsExactly(tableIdentifier);
    assertThat(sessionCatalog.listNamespaces(Namespace.empty())).contains(namespace);
  }

  @Test
  void testFederatedCatalogSessionSurvivesCredentialRefresh()
      throws IOException, InterruptedException {
    int refreshWaitSeconds = refreshWaitSeconds();
    assumeTrue(
        refreshWaitSeconds > 0,
        () ->
            "Set "
                + REFRESH_WAIT_SECONDS_ENV
                + " to a positive number of seconds to enable the BigLake OAuth refresh coverage.");

    RESTCatalog sessionCatalog = newRestCatalogWithClientCredentials();

    Namespace namespace = createTrackedNamespace();
    TableIdentifier tableIdentifier = createTrackedTableIdentifier(namespace);
    String beforeRefreshPayload = "before-refresh-" + UUID.randomUUID();
    String afterRefreshPayload = "after-refresh-" + UUID.randomUUID();

    sessionCatalog.createNamespace(namespace);
    assertThat(sessionCatalog.listNamespaces(Namespace.empty())).contains(namespace);

    Table table = sessionCatalog.createTable(tableIdentifier, SCHEMA);
    createdTables.add(tableIdentifier);
    writePayloadTo(table, beforeRefreshPayload);
    assertThat(rowCountFor(table)).isEqualTo(1L);

    Thread.sleep(refreshWaitSeconds * 1000L);

    Table reloadedTable = sessionCatalog.loadTable(tableIdentifier);
    reloadedTable.refresh();
    assertThat(readPayloads(reloadedTable)).containsExactly(beforeRefreshPayload);

    writePayloadTo(reloadedTable, afterRefreshPayload);
    reloadedTable.refresh();

    assertThat(rowCountFor(reloadedTable)).isEqualTo(2L);
    assertThat(readPayloads(reloadedTable))
        .containsExactlyInAnyOrder(beforeRefreshPayload, afterRefreshPayload);
  }

  private static void validateCloudConfiguration() {
    List<String> missing = new ArrayList<>();
    requireEnv(GOOGLE_APPLICATION_CREDENTIALS, missing);
    requireEnv(SERVICE_ACCOUNT_ENV, missing);
    requireEnv(FEDERATED_CATALOG_ENV, missing);
    requireEnv(QUOTA_PROJECT_ENV, missing);
    requireEnv(BASE_LOCATION_ENV, missing);

    assumeTrue(
        missing.isEmpty(),
        () ->
            "Skipping BigLake cloud integration tests because these environment variables are "
                + "required but not set: "
                + String.join(", ", missing));
  }

  private static void requireEnv(String name, List<String> missing) {
    if (isBlank(System.getenv(name))) {
      missing.add(name);
    }
  }

  private void permissionCatalog() {
    String localCatalogRoleName = ownedEntityName("biglake_catalog_role");
    managementApi.createCatalogRole(localCatalogName, localCatalogRoleName);
    managementApi.addGrant(localCatalogName, localCatalogRoleName, DEFAULT_CATALOG_GRANT);
    CatalogRole localCatalogRole =
        managementApi.getCatalogRole(localCatalogName, localCatalogRoleName);
    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName, localCatalogName, localCatalogRole);
  }

  private void createCatalog() {
    CatalogProperties bucketProperties = new CatalogProperties(BASE_LOCATION);
    bucketProperties.put("enable.credential.vending", "true");

    GcpAuthenticationParameters authenticationParameters =
        new GcpAuthenticationParameters(AuthenticationParameters.AuthenticationTypeEnum.GCP);

    IcebergRestConnectionConfigInfo connectionInfo =
        IcebergRestConnectionConfigInfo.builder()
            .setRemoteCatalogName(FEDERATED_CATALOG)
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setProperties(Map.of("header.x-goog-user-project", QUOTA_PROJECT))
            .setAuthenticationParameters(authenticationParameters)
            .setUri(BIG_LAKE_REST_URI)
            .build();

    ExternalCatalog catalog =
        new ExternalCatalog(
            connectionInfo,
            Catalog.TypeEnum.EXTERNAL,
            localCatalogName,
            bucketProperties,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            null,
            GcpStorageConfigInfo.builder()
                .setGcsServiceAccount(SERVICE_ACCOUNT)
                .setStorageType(StorageConfigInfo.StorageTypeEnum.GCS)
                .setAllowedLocations(List.of(BASE_LOCATION))
                .build());

    managementApi.createCatalog(catalog);
  }

  private RESTCatalog newRestCatalogWithToken(String userToken) {
    return IcebergHelper.restCatalog(
        endpoints,
        localCatalogName,
        Map.of("header.X-Iceberg-Access-Delegation", "vended-credentials"),
        userToken);
  }

  private RESTCatalog newRestCatalogWithClientCredentials() {
    RESTCatalog restCatalog = new RESTCatalog();
    String credentialString =
        userCredentials.credentials().clientId() + ":" + userCredentials.credentials().clientSecret();
    Map<String, String> properties = new java.util.HashMap<>(endpoints.extraHeaders("header."));
    properties.put(org.apache.iceberg.CatalogProperties.URI, endpoints.catalogApiEndpoint().toString());
    properties.put(OAuth2Properties.CREDENTIAL, credentialString);
    properties.put(OAuth2Properties.OAUTH2_SERVER_URI, endpoints.catalogApiEndpoint() + "/v1/oauth/tokens");
    properties.put(OAuth2Properties.SCOPE, OAUTH_SCOPE);
    properties.put("warehouse", localCatalogName);
    properties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
    restCatalog.initialize("polaris", properties);
    return restCatalog;
  }

  private Namespace createTrackedNamespace() {
    Namespace namespace = Namespace.of(ownedEntityName("biglake_ns"));
    createdNamespaces.add(namespace);
    return namespace;
  }

  private TableIdentifier createTrackedTableIdentifier(Namespace namespace) {
    return TableIdentifier.of(namespace, ownedEntityName("biglake_tbl"));
  }

  private void writePayloadTo(Table table, String payload) throws IOException {
    byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);

    @SuppressWarnings("resource")
    FileIO io = table.io();

    URI location =
        URI.create(
            table
                .locationProvider()
                .newDataLocation(
                    String.format(
                        "test-file-%s.txt", UUID.randomUUID().toString().replace("-", ""))));

    OutputFile outputFile = io.newOutputFile(location.toString());
    try (PositionOutputStream outputStream = outputFile.createOrOverwrite()) {
      outputStream.write(payloadBytes);
    }

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(outputFile.location())
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(payloadBytes.length)
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile).commit();
  }

  private List<String> readPayloads(Table table) throws IOException {
    table.refresh();

    List<String> payloads = new ArrayList<>();
    try (CloseableIterable<FileScanTask> plannedFiles = table.newScan().planFiles()) {
      for (FileScanTask plannedFile : plannedFiles) {
        InputFile inputFile = table.io().newInputFile(plannedFile.file().location());
        try (var inputStream = inputFile.newStream()) {
          payloads.add(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
        }
      }
    }

    payloads.sort(Comparator.naturalOrder());
    return payloads;
  }

  private long rowCountFor(Table table) {
    Snapshot currentSnapshot = table.currentSnapshot();
    assertThat(currentSnapshot).isNotNull();

    long totalRows = 0L;
    for (ManifestFile manifest : currentSnapshot.allManifests(table.io())) {
      totalRows += manifest.addedRowsCount() != null ? manifest.addedRowsCount() : 0L;
      totalRows += manifest.existingRowsCount() != null ? manifest.existingRowsCount() : 0L;
    }
    return totalRows;
  }

  private void cleanupTablesAndNamespaces() {
    if (userCredentials == null) {
      return;
    }

    CatalogApi userCatalogApi = client.catalogApi(client.obtainToken(userCredentials));

    for (int i = createdTables.size() - 1; i >= 0; i--) {
      TableIdentifier tableIdentifier = createdTables.get(i);
      bestEffort(
          "drop table " + tableIdentifier,
          () -> userCatalogApi.dropTable(localCatalogName, tableIdentifier));
    }

    for (int i = createdNamespaces.size() - 1; i >= 0; i--) {
      Namespace namespace = createdNamespaces.get(i);
      bestEffort(
          "delete namespace " + namespace,
          () -> userCatalogApi.deleteNamespace(localCatalogName, namespace));
    }
  }

  private String ownedEntityName(String hint) {
    return (client.newEntityName(hint) + "_" + UUID.randomUUID().toString().replace("-", ""))
        .toLowerCase(Locale.ROOT);
  }

  private static int refreshWaitSeconds() {
    String configured = System.getenv(REFRESH_WAIT_SECONDS_ENV);
    if (isBlank(configured)) {
      return 0;
    }

    try {
      return Integer.parseInt(configured);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          REFRESH_WAIT_SECONDS_ENV + " must be an integer number of seconds", e);
    }
  }

  private static boolean preserveResources() {
    String configured = System.getenv(PRESERVE_RESOURCES_ENV);
    return "1".equals(configured)
        || "true".equalsIgnoreCase(configured)
        || "yes".equalsIgnoreCase(configured);
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private void bestEffort(String action, ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (RuntimeException | Error e) {
      LOGGER.warn("Failed to {} during BigLake cleanup", action, e);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run();
  }
}

