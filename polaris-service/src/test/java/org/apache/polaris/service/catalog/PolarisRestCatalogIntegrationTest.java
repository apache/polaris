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

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.ResolvingFileIO;
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
import org.apache.polaris.service.test.PolarisRealm;
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

  protected static final String VIEW_QUERY = "select * from ns1.layer1_table";

  private RESTCatalog restCatalog;
  private String currentCatalogName;
  private String realm;
  private PolarisTestClient userClient;

  private final String catalogBaseLocation =
      S3_BUCKET_BASE + "/" + System.getenv("USER") + "/path/to/data";

  private static final String[] DEFAULT_CATALOG_PROPERTIES = {
    "allow.unstructured.table.location", "true",
    "allow.external.table.location", "true"
  };

  @Retention(RetentionPolicy.RUNTIME)
  private @interface CatalogConfig {
    Catalog.TypeEnum value() default Catalog.TypeEnum.INTERNAL;

    String[] properties() default {
      "allow.unstructured.table.location", "true",
      "allow.external.table.location", "true"
    };
  }

  @Retention(RetentionPolicy.RUNTIME)
  private @interface RestCatalogConfig {
    String[] value() default {};
  }

  @BeforeAll
  public static void setup(@PolarisRealm String realm) throws IOException {
    // Set up test location
    PolarisConnectionExtension.createTestDir(realm);
  }

  @BeforeEach
  public void before(
      TestInfo testInfo,
      PolarisToken adminToken,
      SnowmanCredentials snowmanCredentials,
      @PolarisRealm String realm) {
    this.realm = realm;
    PolarisTestClient adminClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), adminToken.token(), realm);
    String userToken =
        TokenUtils.getTokenFromSecrets(
            EXT.client(),
            EXT.getLocalPort(),
            snowmanCredentials.clientId(),
            snowmanCredentials.clientSecret(),
            realm);
    userClient = new PolarisTestClient(EXT.client(), EXT.getLocalPort(), userToken, realm);
    testInfo
        .getTestMethod()
        .ifPresent(
            method -> {
              currentCatalogName = method.getName() + UUID.randomUUID();
              AwsStorageConfigInfo awsConfigModel =
                  AwsStorageConfigInfo.builder()
                      .setRoleArn(TEST_ROLE_ARN)
                      .setExternalId("externalId")
                      .setUserArn("a:user:arn")
                      .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                      .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
                      .build();
              Optional<CatalogConfig> catalogConfig =
                  testInfo
                      .getTestMethod()
                      .flatMap(m -> Optional.ofNullable(m.getAnnotation(CatalogConfig.class)));

              org.apache.polaris.core.admin.model.CatalogProperties.Builder catalogPropsBuilder =
                  org.apache.polaris.core.admin.model.CatalogProperties.builder(
                      catalogBaseLocation);
              String[] properties =
                  catalogConfig.map(CatalogConfig::properties).orElse(DEFAULT_CATALOG_PROPERTIES);
              for (int i = 0; i < properties.length; i += 2) {
                catalogPropsBuilder.addProperty(properties[i], properties[i + 1]);
              }
              if (!S3_BUCKET_BASE.startsWith("file:/")) {
                catalogPropsBuilder.addProperty(
                    CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, "file:");
              }
              Catalog catalog =
                  PolarisCatalog.builder()
                      .setType(
                          catalogConfig.map(CatalogConfig::value).orElse(Catalog.TypeEnum.INTERNAL))
                      .setName(currentCatalogName)
                      .setProperties(catalogPropsBuilder.build())
                      .setStorageConfigInfo(
                          S3_BUCKET_BASE.startsWith("file:/")
                              ? new FileStorageConfigInfo(
                                  StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://"))
                              : awsConfigModel)
                      .build();
              try (Response response = adminClient.createCatalog(catalog)) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }

              // Create a new CatalogRole that has CATALOG_MANAGE_CONTENT and CATALOG_MANAGE_ACCESS
              CatalogRole newRole = new CatalogRole("custom-admin");
              try (Response response = adminClient.createCatalogRole(currentCatalogName, newRole)) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }
              CatalogGrant grantResource =
                  new CatalogGrant(
                      CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
              try (Response response =
                  adminClient.grantCatalogRole(currentCatalogName, "custom-admin", grantResource)) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }
              CatalogGrant grantAccessResource =
                  new CatalogGrant(
                      CatalogPrivilege.CATALOG_MANAGE_ACCESS, GrantResource.TypeEnum.CATALOG);
              try (Response response =
                  adminClient.grantCatalogRole(
                      currentCatalogName, "custom-admin", grantAccessResource)) {
                assertThat(response)
                    .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
              }

              // Assign this new CatalogRole to the service_admin PrincipalRole
              try (Response response =
                  adminClient.getCatalogRole(currentCatalogName, "custom-admin")) {
                assertThat(response)
                    .returns(Response.Status.OK.getStatusCode(), Response::getStatus);
                CatalogRole catalogRole = response.readEntity(CatalogRole.class);
                try (Response assignResponse =
                    adminClient.grantCatalogRoleToPrincipalRole(
                        "catalog-admin", currentCatalogName, catalogRole)) {
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
              Optional<RestCatalogConfig> restCatalogConfig =
                  testInfo
                      .getTestMethod()
                      .flatMap(m -> Optional.ofNullable(m.getAnnotation(RestCatalogConfig.class)));
              ImmutableMap.Builder<String, String> propertiesBuilder =
                  ImmutableMap.<String, String>builder()
                      .put(
                          CatalogProperties.URI,
                          "http://localhost:" + EXT.getLocalPort() + "/api/catalog")
                      .put(
                          OAuth2Properties.CREDENTIAL,
                          snowmanCredentials.clientId() + ":" + snowmanCredentials.clientSecret())
                      .put(OAuth2Properties.SCOPE, BasePolarisAuthenticator.PRINCIPAL_ROLE_ALL)
                      .put(
                          CatalogProperties.FILE_IO_IMPL,
                          "org.apache.iceberg.inmemory.InMemoryFileIO")
                      .put("warehouse", currentCatalogName)
                      .put("header." + REALM_PROPERTY_KEY, realm);
              restCatalogConfig.ifPresent(
                  config -> {
                    for (int i = 0; i < config.value().length; i += 2) {
                      propertiesBuilder.put(config.value()[i], config.value()[i + 1]);
                    }
                  });
              this.restCatalog.initialize("polaris", propertiesBuilder.buildKeepingLast());
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
    try (Response response = userClient.createCatalogRole(currentCatalogName, catalogRole)) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  private void addGrant(String catalogRoleName, GrantResource grant) {
    try (Response response = userClient.grantResource(currentCatalogName, catalogRoleName, grant)) {
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
    try (Response response = userClient.listGrants(currentCatalogName, "catalogrole1")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(GrantResources.class))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .containsExactlyInAnyOrder(role1Grants.toArray(new GrantResource[0]));
    }

    // List grants for catalogrole2
    try (Response response = userClient.listGrants(currentCatalogName, "catalogrole2")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
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

    try (Response response = userClient.listGrants(currentCatalogName, "catalogrole1")) {
      assertThat(response)
          .returns(Response.Status.OK.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(GrantResources.class))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .containsExactly(expectedGrant);
    }
  }

  @Test
  public void testCreateTableWithOverriddenBaseLocation(PolarisToken adminToken) {
    PolarisTestClient adminClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), adminToken.token(), realm);
    try (Response response = adminClient.getCatalog(currentCatalogName)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog catalog = response.readEntity(Catalog.class);
      Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
      catalogProps.put(
          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
      try (Response updateResponse =
          adminClient.updateCatalog(
              currentCatalogName,
              new UpdateCatalogRequest(
                  catalog.getEntityVersion(), catalogProps, catalog.getStorageConfigInfo()))) {
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
    PolarisTestClient adminClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), adminToken.token(), realm);
    try (Response response = adminClient.getCatalog(currentCatalogName)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog catalog = response.readEntity(Catalog.class);
      Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
      catalogProps.put(
          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
      try (Response updateResponse =
          adminClient.updateCatalog(
              catalog.getName(),
              new UpdateCatalogRequest(
                  catalog.getEntityVersion(), catalogProps, catalog.getStorageConfigInfo()))) {
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
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("because it conflicts with existing table or namespace");
  }

  @Test
  public void testCreateTableWithOverriddenBaseLocationMustResideInNsDirectory(
      PolarisToken adminToken) {
    PolarisTestClient adminClient =
        new PolarisTestClient(EXT.client(), EXT.getLocalPort(), adminToken.token(), realm);
    try (Response response = adminClient.getCatalog(currentCatalogName)) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      Catalog catalog = response.readEntity(Catalog.class);
      Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
      catalogProps.put(
          PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
      try (Response updateResponse =
          adminClient.updateCatalog(
              catalog.getName(),
              new UpdateCatalogRequest(
                  catalog.getEntityVersion(), catalogProps, catalog.getStorageConfigInfo()))) {
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

  /**
   * Create an EXTERNAL catalog. The test configuration, by default, disables access delegation for
   * EXTERNAL catalogs, so register a table and try to load it with the REST client configured to
   * try to fetch vended credentials. Expect a ForbiddenException.
   */
  @CatalogConfig(Catalog.TypeEnum.EXTERNAL)
  @RestCatalogConfig({"header.X-Iceberg-Access-Delegation", "vended-credentials"})
  @Test
  public void testLoadTableWithAccessDelegationForExternalCatalogWithConfigDisabled() {
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            new Schema(List.of(Types.NestedField.of(1, false, "col1", new Types.StringType()))),
            PartitionSpec.unpartitioned(),
            "file:///tmp/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = "file:///tmp/ns1/my_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(fileLocation));
      restCatalog.registerTable(TableIdentifier.of(ns1, "my_table"), fileLocation);
      try {
        Assertions.assertThatThrownBy(
                () -> restCatalog.loadTable(TableIdentifier.of(ns1, "my_table")))
            .isInstanceOf(ForbiddenException.class)
            .hasMessageContaining("Access Delegation is not supported for this catalog");
      } finally {
        resolvingFileIO.deleteFile(fileLocation);
      }
    }
  }

  /**
   * Create an EXTERNAL catalog. The test configuration, by default, disables access delegation for
   * EXTERNAL catalogs. Register a table and attempt to load it WITHOUT access delegation. This
   * should succeed.
   */
  @CatalogConfig(Catalog.TypeEnum.EXTERNAL)
  @Test
  public void testLoadTableWithoutAccessDelegationForExternalCatalogWithConfigDisabled() {
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            new Schema(List.of(Types.NestedField.of(1, false, "col1", new Types.StringType()))),
            PartitionSpec.unpartitioned(),
            "file:///tmp/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = "file:///tmp/ns1/my_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(fileLocation));
      restCatalog.registerTable(TableIdentifier.of(ns1, "my_table"), fileLocation);
      try {
        restCatalog.loadTable(TableIdentifier.of(ns1, "my_table"));
      } finally {
        resolvingFileIO.deleteFile(fileLocation);
      }
    }
  }

  /**
   * Create an EXTERNAL catalog. The test configuration, by default, disables access delegation for
   * EXTERNAL catalogs. However, we set <code>enable.credential.vending</code> to <code>true</code>
   * for this catalog, enabling it. Register a table and attempt to load it WITH access delegation.
   * This should succeed.
   */
  @CatalogConfig(
      value = Catalog.TypeEnum.EXTERNAL,
      properties = {"enable.credential.vending", "true"})
  @RestCatalogConfig({"header.X-Iceberg-Access-Delegation", "vended-credentials"})
  @Test
  public void testLoadTableWithAccessDelegationForExternalCatalogWithConfigEnabledForCatalog() {
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            new Schema(List.of(Types.NestedField.of(1, false, "col1", new Types.StringType()))),
            PartitionSpec.unpartitioned(),
            "file:///tmp/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = "file:///tmp/ns1/my_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(fileLocation));
      restCatalog.registerTable(TableIdentifier.of(ns1, "my_table"), fileLocation);
      try {
        restCatalog.loadTable(TableIdentifier.of(ns1, "my_table"));
      } finally {
        resolvingFileIO.deleteFile(fileLocation);
      }
    }
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
        userClient.sendNotification(currentCatalogName, "ns1", "tbl1", notification)) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(ErrorResponse.class))
          .returns("Cannot update internal catalog via notifications", ErrorResponse::message);
    }

    // NotificationType.VALIDATE should also surface the same error.
    notification.setNotificationType(NotificationType.VALIDATE);
    try (Response response =
        userClient.sendNotification(currentCatalogName, "ns1", "tbl1", notification)) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(ErrorResponse.class))
          .returns("Cannot update internal catalog via notifications", ErrorResponse::message);
    }
  }

  // Test copied from iceberg/core/src/test/java/org/apache/iceberg/rest/TestRESTCatalog.java
  // TODO: If TestRESTCatalog can be refactored to be more usable as a shared base test class,
  // just inherit these test cases from that instead of copying them here.
  @Test
  public void diffAgainstSingleTable() {
    Namespace namespace = Namespace.of("namespace");
    TableIdentifier identifier = TableIdentifier.of(namespace, "multipleDiffsAgainstSingleTable");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    Table table = catalog().buildTable(identifier, SCHEMA).create();
    Transaction transaction = table.newTransaction();

    UpdateSchema updateSchema =
        transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdatePartitionSpec updateSpec =
        transaction.updateSpec().addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expectedSpec = updateSpec.apply();
    updateSpec.commit();

    TableCommit tableCommit =
        TableCommit.create(
            identifier,
            ((BaseTransaction) transaction).startMetadata(),
            ((BaseTransaction) transaction).currentMetadata());

    restCatalog.commitTransaction(tableCommit);

    Table loaded = catalog().loadTable(identifier);
    assertThat(loaded.schema().asStruct()).isEqualTo(expectedSchema.asStruct());
    assertThat(loaded.spec().fields()).isEqualTo(expectedSpec.fields());
  }

  // Test copied from iceberg/core/src/test/java/org/apache/iceberg/rest/TestRESTCatalog.java
  // TODO: If TestRESTCatalog can be refactored to be more usable as a shared base test class,
  // just inherit these test cases from that instead of copying them here.
  @Test
  public void multipleDiffsAgainstMultipleTables() {
    Namespace namespace = Namespace.of("multiDiffNamespace");
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    Table table1 = catalog().buildTable(identifier1, SCHEMA).create();
    Table table2 = catalog().buildTable(identifier2, SCHEMA).create();
    Transaction t1Transaction = table1.newTransaction();
    Transaction t2Transaction = table2.newTransaction();

    UpdateSchema updateSchema =
        t1Transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdateSchema updateSchema2 =
        t2Transaction.updateSchema().addColumn("new_col2", Types.LongType.get());
    Schema expectedSchema2 = updateSchema2.apply();
    updateSchema2.commit();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    restCatalog.commitTransaction(tableCommit1, tableCommit2);

    assertThat(catalog().loadTable(identifier1).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());

    assertThat(catalog().loadTable(identifier2).schema().asStruct())
        .isEqualTo(expectedSchema2.asStruct());
  }

  // Test copied from iceberg/core/src/test/java/org/apache/iceberg/rest/TestRESTCatalog.java
  // TODO: If TestRESTCatalog can be refactored to be more usable as a shared base test class,
  // just inherit these test cases from that instead of copying them here.
  @Test
  public void multipleDiffsAgainstMultipleTablesLastFails() {
    Namespace namespace = Namespace.of("multiDiffNamespace");
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    catalog().createTable(identifier1, SCHEMA);
    catalog().createTable(identifier2, SCHEMA);

    Table table1 = catalog().loadTable(identifier1);
    Table table2 = catalog().loadTable(identifier2);
    Schema originalSchemaOne = table1.schema();

    Transaction t1Transaction = catalog().loadTable(identifier1).newTransaction();
    t1Transaction.updateSchema().addColumn("new_col1", Types.LongType.get()).commit();

    Transaction t2Transaction = catalog().loadTable(identifier2).newTransaction();
    t2Transaction.updateSchema().renameColumn("data", "new-column").commit();

    // delete the colum that is being renamed in the above TX to cause a conflict
    table2.updateSchema().deleteColumn("data").commit();
    Schema updatedSchemaTwo = table2.schema();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    assertThatThrownBy(() -> restCatalog.commitTransaction(tableCommit1, tableCommit2))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: current schema changed: expected id 0 != 1");

    Schema schema1 = catalog().loadTable(identifier1).schema();
    assertThat(schema1.asStruct()).isEqualTo(originalSchemaOne.asStruct());

    Schema schema2 = catalog().loadTable(identifier2).schema();
    assertThat(schema2.asStruct()).isEqualTo(updatedSchemaTwo.asStruct());
    assertThat(schema2.findField("data")).isNull();
    assertThat(schema2.findField("new-column")).isNull();
    assertThat(schema2.columns()).hasSize(1);
  }

  @Test
  public void testMultipleConflictingCommitsToSingleTableInTransaction() {
    Namespace namespace = Namespace.of("ns1");
    TableIdentifier identifier =
        TableIdentifier.of(namespace, "multipleConflictingCommitsAgainstSingleTable");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    // Start two independent transactions on the same base table.
    Table table = catalog().buildTable(identifier, SCHEMA).create();
    Schema originalSchema = catalog().loadTable(identifier).schema();
    Transaction transaction1 = table.newTransaction();
    Transaction transaction2 = table.newTransaction();

    transaction1.updateSchema().renameColumn("data", "new-column1").commit();
    transaction2.updateSchema().renameColumn("data", "new-column2").commit();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier,
            ((BaseTransaction) transaction1).startMetadata(),
            ((BaseTransaction) transaction1).currentMetadata());
    TableCommit tableCommit2 =
        TableCommit.create(
            identifier,
            ((BaseTransaction) transaction2).startMetadata(),
            ((BaseTransaction) transaction2).currentMetadata());

    // "Initial" commit requirements will succeed for both commits being based on the original
    // table but should fail the entire transaction on the second commit.
    assertThatThrownBy(() -> restCatalog.commitTransaction(tableCommit1, tableCommit2))
        .isInstanceOf(CommitFailedException.class);

    // If an implementation validates all UpdateRequirements up-front, then it might pass
    // tests where the UpdateRequirement fails up-front without being atomic. Here we can
    // catch such scenarios where update requirements appear to be fine up-front but will
    // fail when trying to commit the second update, and verify that nothing was actually
    // committed in the end.
    Schema latestCommittedSchema = catalog().loadTable(identifier).schema();
    assertThat(latestCommittedSchema.asStruct()).isEqualTo(originalSchema.asStruct());
  }
}
