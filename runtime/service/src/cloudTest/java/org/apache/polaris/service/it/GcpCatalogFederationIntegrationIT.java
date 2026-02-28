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

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
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
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
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
  private static CatalogApi catalogApi;

  static {
    String googleCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    if (googleCredentials == null) {
      LOGGER.warn("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set");
    } else {
      LOGGER.info("GOOGLE_APPLICATION_CREDENTIALS defined");
    }
  }

  private static final String SERVICE_ACCOUNT =
      System.getenv("INTEGRATION_TEST_GCS_SERVICE_ACCOUNT");
  private static final String FEDERATED_CATALOG =
      System.getenv("INTEGRATION_TEST_GCS_FEDERATED_CATALOG");
  private static final String QUOTA_PROJECT =
      System.getenv("INTEGRATION_TEST_GCS_FEDERATED_QUOTA_PROJECT");
  private static final String BASE_LOCATION = System.getenv("INTEGRATION_TEST_GCS_FEDERATED_PATH");

  private static final String PRINCIPAL_NAME = "test-catalog-federation-user";
  private static final String PRINCIPAL_ROLE_NAME = "test-catalog-federation-user-role";
  private String localCatalogName;

  private static final CatalogGrant DEFAULT_CATALOG_GRANT =
      CatalogGrant.builder()
          .setType(GrantResource.TypeEnum.CATALOG)
          .setPrivilege(CatalogPrivilege.CATALOG_MANAGE_CONTENT)
          .build();

  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private PrincipalWithCredentials newUserCredentials;

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    String adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    catalogApi = client.catalogApi(adminToken);
  }

  @BeforeEach
  void before() {
    setupCatalogs();
  }

  @AfterEach
  void tearDown() {
    managementApi.dropCatalog(localCatalogName);
    managementApi.deletePrincipalRole(PRINCIPAL_ROLE_NAME);
    managementApi.deletePrincipal(PRINCIPAL_NAME);
  }

  private void setupCatalogs() {
    purgePolaris();
    LOGGER.info(
        "federated catalog name = {}, service account = {}, base location = {}, quota project = {}",
        FEDERATED_CATALOG,
        SERVICE_ACCOUNT,
        BASE_LOCATION,
        QUOTA_PROJECT);
    newUserCredentials = managementApi.createPrincipalWithRole(PRINCIPAL_NAME, PRINCIPAL_ROLE_NAME);
    localCatalogName = "test_catalog" + UUID.randomUUID().toString().replace("-", "");
    createCatalog();
    permissionCatalog();
  }

  private void permissionCatalog() {
    String localCatalogRoleName =
        "test-catalog-role_" + UUID.randomUUID().toString().replace("-", "");
    managementApi.createCatalogRole(localCatalogName, localCatalogRoleName);
    managementApi.addGrant(localCatalogName, localCatalogRoleName, DEFAULT_CATALOG_GRANT);
    CatalogRole localCatalogRole =
        managementApi.getCatalogRole(localCatalogName, localCatalogRoleName);
    managementApi.grantCatalogRoleToPrincipalRole(
        PRINCIPAL_ROLE_NAME, localCatalogName, localCatalogRole);
  }

  private void createCatalog() {
    CatalogProperties bucketProperties = new CatalogProperties(BASE_LOCATION);
    bucketProperties.put("enable.credential.vending", "true");

    GcpAuthenticationParameters authenticationParameters =
        new GcpAuthenticationParameters(
            QUOTA_PROJECT, BASE_LOCATION, AuthenticationParameters.AuthenticationTypeEnum.GCP);

    IcebergRestConnectionConfigInfo connectionInfo =
        new IcebergRestConnectionConfigInfo(
            FEDERATED_CATALOG,
            ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST,
            BIG_LAKE_REST_URI,
            authenticationParameters,
            null);

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

  private void purgePolaris() {
    managementApi.listPrincipals().stream()
        .filter(p -> p.getName().equals(PRINCIPAL_NAME))
        .forEach(p -> managementApi.deletePrincipal(p.getName()));
    managementApi.listPrincipalRoles().stream()
        .filter(r -> r.getName().equals(PRINCIPAL_ROLE_NAME))
        .forEach(r -> managementApi.deletePrincipalRole(r.getName()));
  }

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "doc"),
          optional(2, "data", Types.StringType.get()));

  @Test
  void testFederatedCatalogBasicReadWriteOperations() {
    String namespace = "test_namespace" + UUID.randomUUID().toString().replace("-", "");
    String tableName = "test_table";
    catalogApi.createNamespace(localCatalogName, namespace);
    TableIdentifier id = TableIdentifier.of(namespace, tableName);
    RESTCatalog restCatalog = new RESTCatalog();
    String userToken = client.obtainToken(newUserCredentials);
    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(
                org.apache.iceberg.CatalogProperties.URI, endpoints.catalogApiEndpoint().toString())
            .put(OAuth2Properties.TOKEN, userToken)
            .put("warehouse", localCatalogName)
            .put("header.X-Iceberg-Access-Delegation", "vended-credentials")
            .putAll(endpoints.extraHeaders("header."));

    restCatalog.initialize("polaris", propertiesBuilder.buildKeepingLast());
    Table table = restCatalog.createTable(id, SCHEMA);
    assertThat(table).isNotNull();
    assertThat(table.currentSnapshot()).isNull();
    writeRowTo(table);
    assertThat(rowCountFor(table)).isEqualTo(1);
  }

  private void writeRowTo(Table table) {
    @SuppressWarnings("resource")
    FileIO io = table.io();

    URI loc =
        URI.create(
            table
                .locationProvider()
                .newDataLocation(
                    String.format(
                        "test-file-%s.txt", UUID.randomUUID().toString().replace("-", ""))));

    OutputFile f1 = io.newOutputFile(loc.toString());
    String location = f1.location();
    DataFile df =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(location)
            .withFormat(FileFormat.PARQUET) // bogus value
            .withFileSizeInBytes(4)
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(df).commit();
  }

  private long rowCountFor(Table table) {
    Snapshot currentSnapshot = table.currentSnapshot();
    assertThat(currentSnapshot).isNotNull();

    long totalRows = 0;
    for (ManifestFile manifest : currentSnapshot.allManifests(table.io())) {
      totalRows += manifest.addedRowsCount() + manifest.existingRowsCount();
      totalRows -= manifest.deletedRowsCount();
    }
    return totalRows;
  }
}
