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

import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
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
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.TablePrivilege;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.apache.polaris.core.admin.model.ViewPrivilege;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.assertj.core.api.Assertions;
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
 * Import the full core Iceberg catalog tests by hitting the REST service via the RESTCatalog
 * client.
 *
 * @implSpec @implSpec This test expects the server to be configured with the following features
 *     configured:
 *     <ul>
 *       <li>{@link PolarisConfiguration#ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING}: {@code false}
 *     </ul>
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisRestCatalogIntegrationTest extends CatalogTests<RESTCatalog> {
  private static final String TEST_ROLE_ARN =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_ROLE_ARN"))
          .orElse("arn:aws:iam::123456789012:role/my-role");

  private static URI s3BucketBase;
  private static URI externalCatalogBase;

  protected static final String VIEW_QUERY = "select * from ns1.layer1_table";
  private static String principalRoleName;
  private static ClientCredentials adminCredentials;
  private static PrincipalWithCredentials principalCredentials;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static CatalogApi catalogApi;

  private RESTCatalog restCatalog;
  private String currentCatalogName;

  private final String catalogBaseLocation =
      s3BucketBase + "/" + System.getenv("USER") + "/path/to/data";

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
  static void setup(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    adminCredentials = credentials;
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    managementApi = client.managementApi(credentials);
    String principalName = client.newEntityName("snowman-rest");
    principalRoleName = client.newEntityName("rest-admin");
    principalCredentials = managementApi.createPrincipalWithRole(principalName, principalRoleName);
    catalogApi = client.catalogApi(principalCredentials);
    URI testRootUri = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    s3BucketBase = testRootUri.resolve("my-bucket");
    externalCatalogBase = testRootUri.resolve("external-catalog");
  }

  @AfterAll
  static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    String principalName = "snowman-rest-" + UUID.randomUUID();
    principalRoleName = "rest-admin-" + UUID.randomUUID();
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);

    catalogApi = client.catalogApi(principalCredentials);

    Method method = testInfo.getTestMethod().orElseThrow();
    currentCatalogName = client.newEntityName(method.getName());
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn(TEST_ROLE_ARN)
            .setExternalId("externalId")
            .setUserArn("a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    Optional<CatalogConfig> catalogConfig =
        Optional.ofNullable(method.getAnnotation(CatalogConfig.class));

    CatalogProperties.Builder catalogPropsBuilder = CatalogProperties.builder(catalogBaseLocation);
    String[] properties =
        catalogConfig.map(CatalogConfig::properties).orElse(DEFAULT_CATALOG_PROPERTIES);
    for (int i = 0; i < properties.length; i += 2) {
      catalogPropsBuilder.addProperty(properties[i], properties[i + 1]);
    }
    if (!s3BucketBase.getScheme().equals("file")) {
      catalogPropsBuilder.addProperty(
          CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, "file:");
    }
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(catalogConfig.map(CatalogConfig::value).orElse(Catalog.TypeEnum.INTERNAL))
            .setName(currentCatalogName)
            .setProperties(catalogPropsBuilder.build())
            .setStorageConfigInfo(
                s3BucketBase.getScheme().equals("file")
                    ? new FileStorageConfigInfo(
                        StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://"))
                    : awsConfigModel)
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    Optional<PolarisRestCatalogIntegrationTest.RestCatalogConfig> restCatalogConfig =
        testInfo
            .getTestMethod()
            .flatMap(
                m ->
                    Optional.ofNullable(
                        m.getAnnotation(
                            PolarisRestCatalogIntegrationTest.RestCatalogConfig.class)));
    ImmutableMap.Builder<String, String> extraPropertiesBuilder = ImmutableMap.builder();
    restCatalogConfig.ifPresent(
        config -> {
          for (int i = 0; i < config.value().length; i += 2) {
            extraPropertiesBuilder.put(config.value()[i], config.value()[i + 1]);
          }
        });

    restCatalog =
        IcebergHelper.restCatalog(
            client,
            endpoints,
            principalCredentials,
            currentCatalogName,
            extraPropertiesBuilder.build());
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(adminCredentials);
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

    managementApi.createCatalogRole(currentCatalogName, "catalogrole1");
    managementApi.createCatalogRole(currentCatalogName, "catalogrole2");

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
    role1Grants.forEach(grant -> managementApi.addGrant(currentCatalogName, "catalogrole1", grant));
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
    role2Grants.forEach(grant -> managementApi.addGrant(currentCatalogName, "catalogrole2", grant));

    // List grants for catalogrole1
    assertThat(managementApi.listGrants(currentCatalogName, "catalogrole1"))
        .extracting(GrantResources::getGrants)
        .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
        .containsExactlyInAnyOrder(role1Grants.toArray(new GrantResource[0]));

    // List grants for catalogrole2
    assertThat(managementApi.listGrants(currentCatalogName, "catalogrole2"))
        .extracting(GrantResources::getGrants)
        .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
        .containsExactlyInAnyOrder(role2Grants.toArray(new GrantResource[0]));
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

    managementApi.createCatalogRole(currentCatalogName, "catalogrole1");
    managementApi.addGrant(currentCatalogName, "catalogrole1", tableGrant1);

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

    assertThat(managementApi.listGrants(currentCatalogName, "catalogrole1"))
        .extracting(GrantResources::getGrants)
        .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
        .containsExactly(expectedGrant);
  }

  @Test
  public void testCreateTableWithOverriddenBaseLocation() {
    Catalog catalog = managementApi.getCatalog(currentCatalogName);
    Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
    catalogProps.put(
        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
    managementApi.updateCatalog(catalog, catalogProps);

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
  public void testCreateTableWithOverriddenBaseLocationCannotOverlapSibling() {
    Catalog catalog = managementApi.getCatalog(currentCatalogName);
    Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
    catalogProps.put(
        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
    managementApi.updateCatalog(catalog, catalogProps);

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
  public void testCreateTableWithOverriddenBaseLocationMustResideInNsDirectory() {
    Catalog catalog = managementApi.getCatalog(currentCatalogName);
    Map<String, String> catalogProps = new HashMap<>(catalog.getProperties().toMap());
    catalogProps.put(
        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "false");
    managementApi.updateCatalog(catalog, catalogProps);

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
            externalCatalogBase + "/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = externalCatalogBase + "/ns1/my_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(fileLocation));
      restCatalog.registerTable(TableIdentifier.of(ns1, "my_table"), fileLocation);
      try {
        Assertions.assertThatThrownBy(
                () -> restCatalog.loadTable(TableIdentifier.of(ns1, "my_table")))
            .isInstanceOf(ForbiddenException.class)
            .hasMessageContaining("Access Delegation is not enabled for this catalog")
            .hasMessageContaining(
                FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING.catalogConfig());
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
            externalCatalogBase + "/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = externalCatalogBase + "/ns1/my_table/metadata/v1.metadata.json";
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
            externalCatalogBase + "/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = externalCatalogBase + "/ns1/my_table/metadata/v1.metadata.json";
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
    Map<String, String> payload =
        ImmutableMap.<String, String>builder()
            .put("table-name", "tbl1")
            .put("timestamps", "" + System.currentTimeMillis())
            .put("table-uuid", UUID.randomUUID().toString())
            .put("metadata-location", "s3://my-bucket/path/to/metadata.json")
            .build();
    restCatalog.createNamespace(Namespace.of("ns1"));
    Invocation.Builder notificationEndpoint =
        catalogApi.request(
            "v1/{cat}/namespaces/ns1/tables/tbl1/notifications", Map.of("cat", currentCatalogName));
    try (Response response =
        notificationEndpoint.post(
            Entity.json(Map.of("notification-type", "CREATE", "payload", payload)))) {
      assertThat(response)
          .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus)
          .extracting(r -> r.readEntity(ErrorResponse.class))
          .returns("Cannot update internal catalog via notifications", ErrorResponse::message);
    }

    // NotificationType.VALIDATE should also surface the same error.
    try (Response response =
        notificationEndpoint.post(
            Entity.json(Map.of("notification-type", "VALIDATE", "payload", payload)))) {
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

  @Test
  public void testTableExistsStatus() {
    String tableName = "tbl1";
    Namespace namespace = Namespace.of("ns1");
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    catalog().buildTable(identifier, SCHEMA).create();

    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/tables/{table}",
                Map.of("cat", currentCatalogName, "ns", namespace.toString(), "table", tableName))
            .head()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testDropTableStatus() {
    String tableName = "tbl1";
    Namespace namespace = Namespace.of("ns1");
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    catalog().buildTable(identifier, SCHEMA).create();

    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/tables/{table}",
                Map.of("cat", currentCatalogName, "ns", namespace.toString(), "table", tableName))
            .delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testViewExistsStatus() {
    String tableName = "tbl1";
    String viewName = "view1";
    Namespace namespace = Namespace.of("ns1");
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    catalog().buildTable(identifier, SCHEMA).create();

    restCatalog
        .buildView(TableIdentifier.of(namespace, viewName))
        .withSchema(SCHEMA)
        .withDefaultNamespace(namespace)
        .withQuery("spark", VIEW_QUERY)
        .create();

    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/views/{view}",
                Map.of("cat", currentCatalogName, "ns", namespace.toString(), "view", viewName))
            .head()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testDropViewStatus() {
    String tableName = "tbl1";
    String viewName = "view1";
    Namespace namespace = Namespace.of("ns1");
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    catalog().buildTable(identifier, SCHEMA).create();

    restCatalog
        .buildView(TableIdentifier.of(namespace, viewName))
        .withSchema(SCHEMA)
        .withDefaultNamespace(namespace)
        .withQuery("spark", VIEW_QUERY)
        .create();

    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/views/{view}",
                Map.of("cat", currentCatalogName, "ns", namespace.toString(), "view", viewName))
            .delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testRenameViewStatus() {
    String tableName = "tbl1";
    String viewName = "view1";
    String newViewName = "view2";
    Namespace namespace = Namespace.of("ns1");
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    catalog().buildTable(identifier, SCHEMA).create();

    restCatalog
        .buildView(TableIdentifier.of(namespace, viewName))
        .withSchema(SCHEMA)
        .withDefaultNamespace(namespace)
        .withQuery("spark", VIEW_QUERY)
        .create();

    Map<String, Object> payload =
        Map.of(
            "source", Map.of("namespace", List.of(namespace.toString()), "name", viewName),
            "destination", Map.of("namespace", List.of(namespace.toString()), "name", newViewName));

    // Perform view rename
    try (Response response =
        catalogApi
            .request("v1/{cat}/views/rename", Map.of("cat", currentCatalogName))
            .post(Entity.json(payload))) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }

    // Original view should no longer exists
    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/views/{view}",
                Map.of("cat", currentCatalogName, "ns", namespace.toString(), "view", viewName))
            .head()) {
      assertThat(response).returns(Response.Status.NOT_FOUND.getStatusCode(), Response::getStatus);
    }

    // New view should exist
    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/views/{view}",
                Map.of("cat", currentCatalogName, "ns", namespace.toString(), "view", newViewName))
            .head()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testLoadCredentials() {
    String tableName = "tbl1";
    Namespace namespace = Namespace.of("ns1");
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

    catalog().createNamespace(namespace);
    catalog().buildTable(identifier, SCHEMA).create();

    try (Response response =
        catalogApi
            .request(
                "v1/{cat}/namespaces/{ns}/tables/{table}/credentials",
                Map.of("cat", currentCatalogName, "ns", namespace.toString(), "table", tableName))
            .head()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }
  }
}
