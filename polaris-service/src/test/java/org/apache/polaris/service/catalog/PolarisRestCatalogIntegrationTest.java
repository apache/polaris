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
package org.apache.polaris.service.catalog;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.TablePrivilege;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.apache.polaris.core.admin.model.ViewPrivilege;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.auth.BasePolarisAuthenticator;
import org.apache.polaris.service.auth.TokenUtils;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisConnectionExtension.PolarisToken;
import org.apache.polaris.service.test.SnowmanCredentialsExtension;
import org.apache.polaris.service.test.SnowmanCredentialsExtension.SnowmanCredentials;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.apache.polaris.service.types.TableUpdateNotification;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Import the full core Iceberg catalog tests by hitting the REST service via the RESTCatalog
 * client.
 */
@ExtendWith({
  DropwizardExtensionsSupport.class,
  PolarisConnectionExtension.class,
  SnowmanCredentialsExtension.class
})
public class PolarisRestCatalogIntegrationTest extends CatalogTests<RESTCatalog> {
  private static final String TEST_ROLE_ARN =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_ROLE_ARN"))
          .orElse("arn:aws:iam::123456789012:role/my-role");
  private static final String S3_BUCKET_BASE =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_S3_PATH"))
          .orElse("file:///tmp/buckets/my-bucket");
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config(
              "server.adminConnectors[0].port", "0")); // Bind to random port to support parallelism

  protected static final Schema SCHEMA = new Schema(required(4, "data", Types.StringType.get()));
  protected static final String VIEW_QUERY = "select * from ns1.layer1_table";

  private RESTCatalog restCatalog;
  private String currentCatalogName;
  private String userToken;
  private static String realm;

  private final String catalogBaseLocation =
      S3_BUCKET_BASE + "/" + System.getenv("USER") + "/path/to/data";

  @BeforeAll
  public static void setup() throws IOException {
    realm = PolarisConnectionExtension.getTestRealm(PolarisRestCatalogIntegrationTest.class);

    Path testDir = Path.of("build/test_data/iceberg/" + realm);
    FileUtils.deleteQuietly(testDir.toFile());
    Files.createDirectories(testDir);
  }

  @BeforeEach
  public void before(
      TestInfo testInfo, PolarisToken adminToken, SnowmanCredentials snowmanCredentials) {
    userToken =
        TokenUtils.getTokenFromSecrets(
            EXT.client(),
            EXT.getLocalPort(),
            snowmanCredentials.clientId(),
            snowmanCredentials.clientSecret(),
            realm);
    testInfo
        .getTestMethod()
        .ifPresent(
            method -> {
              currentCatalogName = method.getName();
              AwsStorageConfigInfo awsConfigModel =
                  AwsStorageConfigInfo.builder()
                      .setRoleArn(TEST_ROLE_ARN)
                      .setExternalId("externalId")
                      .setUserArn("a:user:arn")
                      .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                      .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
                      .build();
              org.apache.polaris.core.admin.model.CatalogProperties.Builder catalogPropsBuilder =
                  org.apache.polaris.core.admin.model.CatalogProperties.builder(catalogBaseLocation)
                      .addProperty(
                          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                          "true")
                      .addProperty(
                          PolarisConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(),
                          "true");
              if (!S3_BUCKET_BASE.startsWith("file:/")) {
                catalogPropsBuilder.addProperty(
                    CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, "file:");
              }
              Catalog catalog =
                  PolarisCatalog.builder()
                      .setType(Catalog.TypeEnum.INTERNAL)
                      .setName(currentCatalogName)
                      .setProperties(catalogPropsBuilder.build())
                      .setStorageConfigInfo(
                          S3_BUCKET_BASE.startsWith("file:/")
                              ? new FileStorageConfigInfo(
                                  StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://"))
                              : awsConfigModel)
                      .build();
              try (Response response =
                  EXT.client()
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
                      .request("application/json")
                      .header("Authorization", "Bearer " + adminToken.token())
                      .header(REALM_PROPERTY_KEY, realm)
                      .post(Entity.json(catalog))) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }

              // Create a new CatalogRole that has CATALOG_MANAGE_CONTENT and CATALOG_MANAGE_ACCESS
              CatalogRole newRole = new CatalogRole("custom-admin");
              try (Response response =
                  EXT.client()
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles",
                              EXT.getLocalPort(), currentCatalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + adminToken.token())
                      .header(REALM_PROPERTY_KEY, realm)
                      .post(Entity.json(newRole))) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }
              CatalogGrant grantResource =
                  new CatalogGrant(
                      CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
              try (Response response =
                  EXT.client()
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/custom-admin/grants",
                              EXT.getLocalPort(), currentCatalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + adminToken.token())
                      .header(REALM_PROPERTY_KEY, realm)
                      .put(Entity.json(grantResource))) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }
              CatalogGrant grantAccessResource =
                  new CatalogGrant(
                      CatalogPrivilege.CATALOG_MANAGE_ACCESS, GrantResource.TypeEnum.CATALOG);
              try (Response response =
                  EXT.client()
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/custom-admin/grants",
                              EXT.getLocalPort(), currentCatalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + adminToken.token())
                      .header(REALM_PROPERTY_KEY, realm)
                      .put(Entity.json(grantAccessResource))) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }

              // Assign this new CatalogRole to the service_admin PrincipalRole
              try (Response response =
                  EXT.client()
                      .target(
                          String.format(
                              "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/custom-admin",
                              EXT.getLocalPort(), currentCatalogName))
                      .request("application/json")
                      .header("Authorization", "Bearer " + adminToken.token())
                      .header(REALM_PROPERTY_KEY, realm)
                      .get()) {
                assertThat(response)
                    .returns(Response.Status.OK.getStatusCode(), Response::getStatus);
                CatalogRole catalogRole = response.readEntity(CatalogRole.class);
                try (Response assignResponse =
                    EXT.client()
                        .target(
                            String.format(
                                "http://localhost:%d/api/management/v1/principal-roles/catalog-admin/catalog-roles/%s",
                                EXT.getLocalPort(), currentCatalogName))
                        .request("application/json")
                        .header("Authorization", "Bearer " + adminToken.token())
                        .header(REALM_PROPERTY_KEY, realm)
                        .put(Entity.json(catalogRole))) {
                  assertThat(assignResponse)
                      .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
                }
              }

              SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
              this.restCatalog =
                  new RESTCatalog(
                      context,
                      (config) ->
                          HTTPClient.builder(config)
                              .uri(config.get(CatalogProperties.URI))
                              .build());
              this.restCatalog.initialize(
                  "polaris",
                  ImmutableMap.of(
                      CatalogProperties.URI,
                      "http://localhost:" + EXT.getLocalPort() + "/api/catalog",
                      OAuth2Properties.CREDENTIAL,
                      snowmanCredentials.clientId() + ":" + snowmanCredentials.clientSecret(),
                      OAuth2Properties.SCOPE,
                      BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL,
                      CatalogProperties.FILE_IO_IMPL,
                      "org.apache.iceberg.inmemory.InMemoryFileIO",
                      "warehouse",
                      currentCatalogName,
                      "header." + REALM_PROPERTY_KEY,
                      realm));
            });
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean overridesRequestedLocation() {
    return true;
  }

  private void createCatalogRole(String catalogRoleName) {
    CatalogRole catalogRole = new CatalogRole(catalogRoleName);
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles",
                    EXT.getLocalPort(), currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(catalogRole))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  private void addGrant(String catalogRoleName, GrantResource grant) {
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    EXT.getLocalPort(), currentCatalogName, catalogRoleName))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .put(Entity.json(grant))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testListGrantsOnCatalogObjectsToCatalogRoles() {
    restCatalog.createNamespace(Namespace.of("ns1"));
    restCatalog.createNamespace(Namespace.of("ns1", "ns1a"));
    restCatalog.createNamespace(Namespace.of("ns2"));

    restCatalog.buildTable(TableIdentifier.of(Namespace.of("ns1"), "tbl1"), SCHEMA).create();
    restCatalog
        .buildTable(TableIdentifier.of(Namespace.of("ns1", "ns1a"), "tbl1"), SCHEMA)
        .create();
    restCatalog.buildTable(TableIdentifier.of(Namespace.of("ns2"), "tbl2"), SCHEMA).create();

    restCatalog
        .buildView(TableIdentifier.of(Namespace.of("ns1"), "view1"))
        .withSchema(SCHEMA)
        .withDefaultNamespace(Namespace.of("ns1"))
        .withQuery("spark", VIEW_QUERY)
        .create();
    restCatalog
        .buildView(TableIdentifier.of(Namespace.of("ns1", "ns1a"), "view1"))
        .withSchema(SCHEMA)
        .withDefaultNamespace(Namespace.of("ns1"))
        .withQuery("spark", VIEW_QUERY)
        .create();
    restCatalog
        .buildView(TableIdentifier.of(Namespace.of("ns2"), "view2"))
        .withSchema(SCHEMA)
        .withDefaultNamespace(Namespace.of("ns1"))
        .withQuery("spark", VIEW_QUERY)
        .create();

    CatalogGrant catalogGrant1 =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);

    CatalogGrant catalogGrant2 =
        new CatalogGrant(CatalogPrivilege.NAMESPACE_FULL_METADATA, GrantResource.TypeEnum.CATALOG);

    CatalogGrant catalogGrant3 =
        new CatalogGrant(CatalogPrivilege.VIEW_FULL_METADATA, GrantResource.TypeEnum.CATALOG);

    NamespaceGrant namespaceGrant1 =
        new NamespaceGrant(
            List.of("ns1"),
            NamespacePrivilege.NAMESPACE_FULL_METADATA,
            GrantResource.TypeEnum.NAMESPACE);

    NamespaceGrant namespaceGrant2 =
        new NamespaceGrant(
            List.of("ns1", "ns1a"),
            NamespacePrivilege.TABLE_CREATE,
            GrantResource.TypeEnum.NAMESPACE);

    NamespaceGrant namespaceGrant3 =
        new NamespaceGrant(
            List.of("ns2"),
            NamespacePrivilege.VIEW_READ_PROPERTIES,
            GrantResource.TypeEnum.NAMESPACE);

    TableGrant tableGrant1 =
        new TableGrant(
            List.of("ns1"),
            "tbl1",
            TablePrivilege.TABLE_FULL_METADATA,
            GrantResource.TypeEnum.TABLE);

    TableGrant tableGrant2 =
        new TableGrant(
            List.of("ns1", "ns1a"),
            "tbl1",
            TablePrivilege.TABLE_READ_DATA,
            GrantResource.TypeEnum.TABLE);

    TableGrant tableGrant3 =
        new TableGrant(
            List.of("ns2"), "tbl2", TablePrivilege.TABLE_WRITE_DATA, GrantResource.TypeEnum.TABLE);

    ViewGrant viewGrant1 =
        new ViewGrant(
            List.of("ns1"), "view1", ViewPrivilege.VIEW_FULL_METADATA, GrantResource.TypeEnum.VIEW);

    ViewGrant viewGrant2 =
        new ViewGrant(
            List.of("ns1", "ns1a"),
            "view1",
            ViewPrivilege.VIEW_READ_PROPERTIES,
            GrantResource.TypeEnum.VIEW);

    ViewGrant viewGrant3 =
        new ViewGrant(
            List.of("ns2"),
            "view2",
            ViewPrivilege.VIEW_WRITE_PROPERTIES,
            GrantResource.TypeEnum.VIEW);

    createCatalogRole("catalogrole1");
    createCatalogRole("catalogrole2");

    List<GrantResource> role1Grants =
        List.of(
            catalogGrant1,
            catalogGrant2,
            namespaceGrant1,
            namespaceGrant2,
            tableGrant1,
            tableGrant2,
            viewGrant1,
            viewGrant2);
    role1Grants.forEach(grant -> addGrant("catalogrole1", grant));
    List<GrantResource> role2Grants =
        List.of(
            catalogGrant1,
            catalogGrant3,
            namespaceGrant1,
            namespaceGrant3,
            tableGrant1,
            tableGrant3,
            viewGrant1,
            viewGrant3);
    role2Grants.forEach(grant -> addGrant("catalogrole2", grant));

    // List grants for catalogrole1
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    EXT.getLocalPort(), currentCatalogName, "catalogrole1"))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(GrantResources.class))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .containsExactlyInAnyOrder(role1Grants.toArray(new GrantResource[0]));
    }

    // List grants for catalogrole2
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    EXT.getLocalPort(), currentCatalogName, "catalogrole2"))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(GrantResources.class))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .containsExactlyInAnyOrder(role2Grants.toArray(new GrantResource[0]));
    }
  }

  @Test
  public void testListGrantsAfterRename() {
    restCatalog.createNamespace(Namespace.of("ns1"));
    restCatalog.createNamespace(Namespace.of("ns1", "ns1a"));
    restCatalog.createNamespace(Namespace.of("ns2"));

    restCatalog
        .buildTable(TableIdentifier.of(Namespace.of("ns1", "ns1a"), "tbl1"), SCHEMA)
        .create();

    TableGrant tableGrant1 =
        new TableGrant(
            List.of("ns1", "ns1a"),
            "tbl1",
            TablePrivilege.TABLE_FULL_METADATA,
            GrantResource.TypeEnum.TABLE);

    createCatalogRole("catalogrole1");
    addGrant("catalogrole1", tableGrant1);

    // Grants will follow the table through the rename
    restCatalog.renameTable(
        TableIdentifier.of(Namespace.of("ns1", "ns1a"), "tbl1"),
        TableIdentifier.of(Namespace.of("ns2"), "newtable"));

    TableGrant expectedGrant =
        new TableGrant(
            List.of("ns2"),
            "newtable",
            TablePrivilege.TABLE_FULL_METADATA,
            GrantResource.TypeEnum.TABLE);

    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s/catalog-roles/%s/grants",
                    EXT.getLocalPort(), currentCatalogName, "catalogrole1"))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response)
          .returns(200, Response::getStatus)
          .extracting(r -> r.readEntity(GrantResources.class))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .containsExactly(expectedGrant);
    }
  }

  @Test
  public void testCreateTableWithOverriddenBaseLocation(PolarisToken adminToken) {
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s",
                    EXT.getLocalPort(), currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + adminToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog catalog = response.readEntity(Catalog.class);
      Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
      catalogProps.put(
          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
      try (Response updateResponse =
          EXT.client()
              .target(
                  String.format(
                      "http://localhost:%d/api/management/v1/catalogs/%s",
                      EXT.getLocalPort(), catalog.getName()))
              .request("application/json")
              .header("Authorization", "Bearer " + adminToken.token())
              .header(REALM_PROPERTY_KEY, realm)
              .put(
                  Entity.json(
                      new UpdateCatalogRequest(
                          catalog.getEntityVersion(),
                          catalogProps,
                          catalog.getStorageConfigInfo())))) {
        assertThat(updateResponse).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      }
    }

    restCatalog.createNamespace(Namespace.of("ns1"));
    restCatalog.createNamespace(
        Namespace.of("ns1", "ns1a"),
        ImmutableMap.of(
            PolarisEntityConstants.ENTITY_BASE_LOCATION,
            catalogBaseLocation + "/ns1/ns1a-override"));

    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("ns1", "ns1a"), "tbl1");
    restCatalog
        .buildTable(tableIdentifier, SCHEMA)
        .withLocation(catalogBaseLocation + "/ns1/ns1a-override/tbl1-override")
        .create();
    Table table = restCatalog.loadTable(tableIdentifier);
    assertThat(table)
        .isNotNull()
        .isInstanceOf(BaseTable.class)
        .asInstanceOf(InstanceOfAssertFactories.type(BaseTable.class))
        .returns(catalogBaseLocation + "/ns1/ns1a-override/tbl1-override", BaseTable::location);
  }

  @Test
  public void testCreateTableWithOverriddenBaseLocationCannotOverlapSibling(
      PolarisToken adminToken) {
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s",
                    EXT.getLocalPort(), currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + adminToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog catalog = response.readEntity(Catalog.class);
      Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
      catalogProps.put(
          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
      try (Response updateResponse =
          EXT.client()
              .target(
                  String.format(
                      "http://localhost:%d/api/management/v1/catalogs/%s",
                      EXT.getLocalPort(), catalog.getName()))
              .request("application/json")
              .header("Authorization", "Bearer " + adminToken.token())
              .header(REALM_PROPERTY_KEY, realm)
              .put(
                  Entity.json(
                      new UpdateCatalogRequest(
                          catalog.getEntityVersion(),
                          catalogProps,
                          catalog.getStorageConfigInfo())))) {
        assertThat(updateResponse).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      }
    }

    restCatalog.createNamespace(Namespace.of("ns1"));
    restCatalog.createNamespace(
        Namespace.of("ns1", "ns1a"),
        ImmutableMap.of(
            PolarisEntityConstants.ENTITY_BASE_LOCATION,
            catalogBaseLocation + "/ns1/ns1a-override"));

    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("ns1", "ns1a"), "tbl1");
    restCatalog
        .buildTable(tableIdentifier, SCHEMA)
        .withLocation(catalogBaseLocation + "/ns1/ns1a-override/tbl1-override")
        .create();
    Table table = restCatalog.loadTable(tableIdentifier);
    assertThat(table)
        .isNotNull()
        .isInstanceOf(BaseTable.class)
        .asInstanceOf(InstanceOfAssertFactories.type(BaseTable.class))
        .returns(catalogBaseLocation + "/ns1/ns1a-override/tbl1-override", BaseTable::location);

    Assertions.assertThatThrownBy(
            () ->
                restCatalog
                    .buildTable(TableIdentifier.of(Namespace.of("ns1", "ns1a"), "tbl2"), SCHEMA)
                    .withLocation(catalogBaseLocation + "/ns1/ns1a-override/tbl1-override")
                    .create())
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("because it conflicts with existing table or namespace");
  }

  @Test
  public void testCreateTableWithOverriddenBaseLocationMustResideInNsDirectory(
      PolarisToken adminToken) {
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/%s",
                    EXT.getLocalPort(), currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + adminToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog catalog = response.readEntity(Catalog.class);
      Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
      catalogProps.put(
          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
      try (Response updateResponse =
          EXT.client()
              .target(
                  String.format(
                      "http://localhost:%d/api/management/v1/catalogs/%s",
                      EXT.getLocalPort(), catalog.getName()))
              .request("application/json")
              .header("Authorization", "Bearer " + adminToken.token())
              .header(REALM_PROPERTY_KEY, realm)
              .put(
                  Entity.json(
                      new UpdateCatalogRequest(
                          catalog.getEntityVersion(),
                          catalogProps,
                          catalog.getStorageConfigInfo())))) {
        assertThat(updateResponse).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      }
    }

    restCatalog.createNamespace(Namespace.of("ns1"));
    restCatalog.createNamespace(
        Namespace.of("ns1", "ns1a"),
        ImmutableMap.of(
            PolarisEntityConstants.ENTITY_BASE_LOCATION,
            catalogBaseLocation + "/ns1/ns1a-override"));

    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("ns1", "ns1a"), "tbl1");
    assertThatThrownBy(
            () ->
                restCatalog
                    .buildTable(tableIdentifier, SCHEMA)
                    .withLocation(catalogBaseLocation + "/ns1/ns1a/tbl1-override")
                    .create())
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  public void testSendNotificationInternalCatalog() {
    NotificationRequest notification = new NotificationRequest();
    notification.setNotificationType(NotificationType.CREATE);
    notification.setPayload(
        new TableUpdateNotification(
            "tbl1",
            System.currentTimeMillis(),
            UUID.randomUUID().toString(),
            "s3://my-bucket/path/to/metadata.json",
            null));
    restCatalog.createNamespace(Namespace.of("ns1"));
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/catalog/v1/%s/namespaces/ns1/tables/tbl1/notifications",
                    EXT.getLocalPort(), currentCatalogName))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(notification))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(ErrorResponse.class))
          .returns("Cannot update internal catalog via notifications", ErrorResponse::message);
    }
  }
}
