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

import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
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
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
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
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.GenericTableApi;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.GenericTable;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.configuration.PreferredAssumptionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Import the full core Iceberg catalog tests by hitting the REST service via the RESTCatalog
 * client.
 *
 * @implSpec @implSpec This test expects the server to be configured with the following features
 *     configured:
 *     <ul>
 *       <li>{@link FeatureConfiguration#ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING}: {@code false}
 *     </ul>
 *     CODE_COPIED_TO_POLARIS From Apache Iceberg Version: 1.7.1
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public abstract class PolarisRestCatalogIntegrationBase extends CatalogTests<RESTCatalog> {
  protected static final String VIEW_QUERY = "select * from ns1.layer1_table";
  // subpath shouldn't start with a slash, as it is appended to the base URI
  private static final String CATALOG_LOCATION_SUBPATH =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_CATALOG_LOCATION_SUBPATH"))
          .orElse("path/to/data");
  private static final String EXTERNAL_CATALOG_LOCATION_SUBPATH =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_EXTERNAL_CATALOG_LOCATION_SUBPATH"))
          .orElse("external-catalog");
  private static ClientCredentials adminCredentials;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;

  private PrincipalWithCredentials principalCredentials;
  private CatalogApi catalogApi;
  private GenericTableApi genericTableApi;
  private RESTCatalog restCatalog;
  private String currentCatalogName;
  private Map<String, String> restCatalogConfig;
  private URI externalCatalogBase;
  private String catalogBaseLocation;

  private static final Map<String, String> DEFAULT_REST_CATALOG_CONFIG =
      Map.of(
          org.apache.iceberg.CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1",
          "catalog-default-key1",
          org.apache.iceberg.CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2",
          "catalog-default-key2",
          org.apache.iceberg.CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
          "catalog-default-key3",
          org.apache.iceberg.CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
          "catalog-override-key3",
          org.apache.iceberg.CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
          "catalog-override-key4");

  private static final String[] DEFAULT_CATALOG_PROPERTIES = {
    "polaris.config.allow.unstructured.table.location", "true",
    "polaris.config.allow.external.table.location", "true",
    "polaris.config.list-pagination-enabled", "true"
  };

  @Retention(RetentionPolicy.RUNTIME)
  private @interface CatalogConfig {
    Catalog.TypeEnum value() default Catalog.TypeEnum.INTERNAL;

    String[] properties() default {
      "polaris.config.allow.unstructured.table.location", "true",
      "polaris.config.allow.external.table.location", "true"
    };
  }

  @Retention(RetentionPolicy.RUNTIME)
  private @interface RestCatalogConfig {
    String[] value() default {};
  }

  /**
   * Get the storage configuration information for the catalog.
   *
   * @return StorageConfigInfo instance containing the storage configuration
   */
  protected abstract StorageConfigInfo getStorageConfigInfo();

  /**
   * Determine whether the test should be skipped based on the environment or configuration.
   *
   * @return true if the test should be skipped, false otherwise
   */
  protected abstract boolean shouldSkip();

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    adminCredentials = credentials;
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    managementApi = client.managementApi(credentials);
  }

  static {
    Assumptions.setPreferredAssumptionException(PreferredAssumptionException.JUNIT5);
  }

  @AfterAll
  static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    Assumptions.assumeThat(shouldSkip()).isFalse();
    String principalName = client.newEntityName("snowman-rest");
    String principalRoleName = client.newEntityName("rest-admin");
    principalCredentials = managementApi.createPrincipalWithRole(principalName, principalRoleName);

    catalogApi = client.catalogApi(principalCredentials);
    genericTableApi = client.genericTableApi(principalCredentials);

    Method method = testInfo.getTestMethod().orElseThrow();
    currentCatalogName = client.newEntityName(method.getName());
    StorageConfigInfo storageConfig = getStorageConfigInfo();
    URI testRuntimeURI = URI.create(storageConfig.getAllowedLocations().getFirst());
    catalogBaseLocation = testRuntimeURI + "/" + CATALOG_LOCATION_SUBPATH;
    externalCatalogBase =
        URI.create(
            testRuntimeURI + "/" + EXTERNAL_CATALOG_LOCATION_SUBPATH + "/" + method.getName());

    Optional<CatalogConfig> catalogConfig =
        Optional.ofNullable(method.getAnnotation(CatalogConfig.class));

    CatalogProperties.Builder catalogPropsBuilder = CatalogProperties.builder(catalogBaseLocation);
    String[] properties =
        catalogConfig.map(CatalogConfig::properties).orElse(DEFAULT_CATALOG_PROPERTIES);
    for (int i = 0; i < properties.length; i += 2) {
      catalogPropsBuilder.addProperty(properties[i], properties[i + 1]);
    }
    catalogPropsBuilder.addProperty(
        FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "true");
    if (!testRuntimeURI.getScheme().equals("file")) {
      catalogPropsBuilder.addProperty(
          CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, "file:");
    }
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(catalogConfig.map(CatalogConfig::value).orElse(Catalog.TypeEnum.INTERNAL))
            .setName(currentCatalogName)
            .setProperties(catalogPropsBuilder.build())
            .setStorageConfigInfo(storageConfig)
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    Map<String, String> dynamicConfig =
        testInfo
            .getTestMethod()
            .map(m -> m.getAnnotation(RestCatalogConfig.class))
            .map(RestCatalogConfig::value)
            .map(
                values -> {
                  if (values.length % 2 != 0) {
                    throw new IllegalArgumentException(
                        String.format("Missing value for config '%s'", values[values.length - 1]));
                  }
                  Map<String, String> config = new HashMap<>();
                  for (int i = 0; i < values.length; i += 2) {
                    config.put(values[i], values[i + 1]);
                  }
                  return config;
                })
            .orElse(ImmutableMap.of());

    restCatalogConfig =
        ImmutableMap.<String, String>builder()
            .putAll(DEFAULT_REST_CATALOG_CONFIG)
            .putAll(dynamicConfig)
            .build();

    restCatalog = initCatalog(currentCatalogName, ImmutableMap.of());
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(adminCredentials);
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  /**
   * Initialize a RESTCatalog for testing.
   *
   * @param catalogName this parameter is currently unused.
   * @param additionalProperties additional properties to apply on top of the default test settings
   * @return a configured instance of RESTCatalog
   */
  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    ImmutableMap.Builder<String, String> extraPropertiesBuilder = ImmutableMap.builder();
    extraPropertiesBuilder.putAll(restCatalogConfig);
    extraPropertiesBuilder.putAll(additionalProperties);
    return IcebergHelper.restCatalog(
        client,
        endpoints,
        principalCredentials,
        currentCatalogName,
        extraPropertiesBuilder.buildKeepingLast());
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
            new Schema(List.of(Types.NestedField.required(1, "col1", new Types.StringType()))),
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
            new Schema(List.of(Types.NestedField.required(1, "col1", new Types.StringType()))),
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
            new Schema(List.of(Types.NestedField.required(1, "col1", new Types.StringType()))),
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
   * Register a table. Then, invoke an initial loadTable request to fetch and ensure ETag is
   * present. Then, invoke a second loadTable to ensure that ETag is matched.
   */
  @Test
  public void testLoadTableTwiceWithETag() {
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            new Schema(List.of(Types.NestedField.required(1, "col1", new Types.StringType()))),
            PartitionSpec.unpartitioned(),
            externalCatalogBase + "/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = externalCatalogBase + "/ns1/my_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(fileLocation));
      restCatalog.registerTable(TableIdentifier.of(ns1, "my_table_etagged"), fileLocation);
      Invocation invocation =
          catalogApi
              .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/my_table_etagged")
              .build("GET");
      try (Response initialLoadTable = invocation.invoke()) {
        assertThat(initialLoadTable.getHeaders()).containsKey(HttpHeaders.ETAG);
        String etag = initialLoadTable.getHeaders().getFirst(HttpHeaders.ETAG).toString();

        Invocation etaggedInvocation =
            catalogApi
                .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/my_table_etagged")
                .header(HttpHeaders.IF_NONE_MATCH, etag)
                .build("GET");

        try (Response etaggedLoadTable = etaggedInvocation.invoke()) {
          assertThat(etaggedLoadTable.getStatus())
              .isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
        }
      } finally {
        resolvingFileIO.deleteFile(fileLocation);
      }
    }
  }

  /**
   * Invoke an initial registerTable request to fetch and ensure ETag is present. Then, invoke a
   * second loadTable to ensure that ETag is matched.
   */
  @Test
  public void testRegisterAndLoadTableWithReturnedETag() {
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            new Schema(List.of(Types.NestedField.required(1, "col1", new Types.StringType()))),
            PartitionSpec.unpartitioned(),
            externalCatalogBase + "/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = externalCatalogBase + "/ns1/my_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(fileLocation));

      Invocation registerInvocation =
          catalogApi
              .request("v1/" + currentCatalogName + "/namespaces/ns1/register")
              .buildPost(
                  Entity.json(
                      Map.of("name", "my_etagged_table", "metadata-location", fileLocation)));
      try (Response registerResponse = registerInvocation.invoke()) {
        assertThat(registerResponse.getHeaders()).containsKey(HttpHeaders.ETAG);
        String etag = registerResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();

        Invocation etaggedInvocation =
            catalogApi
                .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/my_etagged_table")
                .header(HttpHeaders.IF_NONE_MATCH, etag)
                .build("GET");

        try (Response etaggedLoadTable = etaggedInvocation.invoke()) {
          assertThat(etaggedLoadTable.getStatus())
              .isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
        }

      } finally {
        resolvingFileIO.deleteFile(fileLocation);
      }
    }
  }

  @Test
  public void testCreateAndLoadTableWithReturnedEtag() {
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            new Schema(List.of(Types.NestedField.required(1, "col1", new Types.StringType()))),
            PartitionSpec.unpartitioned(),
            externalCatalogBase + "/ns1/my_table",
            Map.of());
    try (ResolvingFileIO resolvingFileIO = new ResolvingFileIO()) {
      resolvingFileIO.initialize(Map.of());
      resolvingFileIO.setConf(new Configuration());
      String fileLocation = externalCatalogBase + "/ns1/my_table/metadata/v1.metadata.json";
      TableMetadataParser.write(tableMetadata, resolvingFileIO.newOutputFile(fileLocation));

      Invocation createInvocation =
          catalogApi
              .request("v1/" + currentCatalogName + "/namespaces/ns1/tables")
              .buildPost(
                  Entity.json(
                      CreateTableRequest.builder()
                          .withName("my_etagged_table")
                          .withLocation(tableMetadata.location())
                          .withPartitionSpec(tableMetadata.spec())
                          .withSchema(tableMetadata.schema())
                          .withWriteOrder(tableMetadata.sortOrder())
                          .build()));
      try (Response createResponse = createInvocation.invoke()) {
        assertThat(createResponse.getHeaders()).containsKey(HttpHeaders.ETAG);
        String etag = createResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();

        Invocation etaggedInvocation =
            catalogApi
                .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/my_etagged_table")
                .header(HttpHeaders.IF_NONE_MATCH, etag)
                .build("GET");

        try (Response etaggedLoadTable = etaggedInvocation.invoke()) {
          assertThat(etaggedLoadTable.getStatus())
              .isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
        }
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

  @Test
  public void testCreateGenericTable() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl1");

    try {
      GenericTable createResponse =
          genericTableApi.createGenericTable(
              currentCatalogName, tableIdentifier, "format", Map.of());
      Assertions.assertThat(createResponse.getFormat()).isEqualTo("format");
    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testLoadGenericTable() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl1");

    try {
      genericTableApi.createGenericTable(currentCatalogName, tableIdentifier, "format", Map.of());

      GenericTable loadResponse =
          genericTableApi.getGenericTable(currentCatalogName, tableIdentifier);
      Assertions.assertThat(loadResponse.getFormat()).isEqualTo("format");

    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testListGenericTables() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    TableIdentifier tableIdentifier1 = TableIdentifier.of(namespace, "tbl1");
    TableIdentifier tableIdentifier2 = TableIdentifier.of(namespace, "tbl2");

    try {
      genericTableApi.createGenericTable(currentCatalogName, tableIdentifier1, "format", Map.of());
      genericTableApi.createGenericTable(currentCatalogName, tableIdentifier2, "format", Map.of());

      List<TableIdentifier> identifiers =
          genericTableApi.listGenericTables(currentCatalogName, namespace);

      Assertions.assertThat(identifiers).hasSize(2);
      Assertions.assertThat(identifiers)
          .containsExactlyInAnyOrder(tableIdentifier1, tableIdentifier2);
    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testDropGenericTable() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl1");

    try {
      genericTableApi.createGenericTable(currentCatalogName, tableIdentifier, "format", Map.of());

      GenericTable loadResponse =
          genericTableApi.getGenericTable(currentCatalogName, tableIdentifier);
      Assertions.assertThat(loadResponse.getFormat()).isEqualTo("format");

      genericTableApi.dropGenericTable(currentCatalogName, tableIdentifier);

      assertThatCode(() -> genericTableApi.getGenericTable(currentCatalogName, tableIdentifier))
          .isInstanceOf(ProcessingException.class);

    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testGrantsOnGenericTable() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);

    try {
      TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl1");
      genericTableApi.createGenericTable(currentCatalogName, tableIdentifier, "format", Map.of());

      managementApi.createCatalogRole(currentCatalogName, "catalogrole1");

      Stream<TableGrant> tableGrants =
          Arrays.stream(TablePrivilege.values())
              .map(
                  p -> {
                    return new TableGrant(List.of("ns1"), "tbl1", p, GrantResource.TypeEnum.TABLE);
                  });

      tableGrants.forEach(g -> managementApi.addGrant(currentCatalogName, "catalogrole1", g));

    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testGrantsOnNonExistingGenericTable() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);

    try {
      managementApi.createCatalogRole(currentCatalogName, "catalogrole1");

      Stream<TableGrant> tableGrants =
          Arrays.stream(TablePrivilege.values())
              .map(
                  p -> {
                    return new TableGrant(List.of("ns1"), "tbl1", p, GrantResource.TypeEnum.TABLE);
                  });

      tableGrants.forEach(
          g -> {
            try (Response response =
                managementApi
                    .request(
                        "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                        Map.of("cat", currentCatalogName, "role", "catalogrole1"))
                    .put(Entity.json(g))) {

              assertThat(response.getStatus()).isEqualTo(NOT_FOUND.getStatusCode());
            }
          });
    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testDropNonExistingGenericTable() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl1");

    try {
      String ns = RESTUtil.encodeNamespace(tableIdentifier.namespace());
      try (Response res =
          genericTableApi
              .request(
                  "polaris/v1/{cat}/namespaces/{ns}/generic-tables/{table}",
                  Map.of("cat", currentCatalogName, "table", tableIdentifier.name(), "ns", ns))
              .delete()) {
        assertThat(res.getStatus()).isEqualTo(NOT_FOUND.getStatusCode());
      }
    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testLoadTableWithSnapshots() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    try {
      TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl1");
      restCatalog.createTable(tableIdentifier, SCHEMA);

      assertThatCode(() -> catalogApi.loadTable(currentCatalogName, tableIdentifier, "ALL"))
          .doesNotThrowAnyException();
      assertThatCode(() -> catalogApi.loadTable(currentCatalogName, tableIdentifier, "all"))
          .doesNotThrowAnyException();
      assertThatCode(() -> catalogApi.loadTable(currentCatalogName, tableIdentifier, "refs"))
          .doesNotThrowAnyException();
      assertThatCode(() -> catalogApi.loadTable(currentCatalogName, tableIdentifier, "REFS"))
          .doesNotThrowAnyException();
      assertThatCode(() -> catalogApi.loadTable(currentCatalogName, tableIdentifier, "not-real"))
          .isInstanceOf(RESTException.class)
          .hasMessageContaining("Unrecognized snapshots")
          .hasMessageContaining("code=" + Response.Status.BAD_REQUEST.getStatusCode());
    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testLoadTableWithRefFiltering() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    try {
      TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl1");

      restCatalog.createTable(tableIdentifier, SCHEMA);

      Table table = restCatalog.loadTable(tableIdentifier);

      // Create an orphaned snapshot:
      table.newAppend().appendFile(FILE_A).commit();
      long snapshotIdA = table.currentSnapshot().snapshotId();
      table.newAppend().appendFile(FILE_B).commit();
      table.manageSnapshots().setCurrentSnapshot(snapshotIdA).commit();

      var allSnapshots =
          catalogApi
              .loadTable(currentCatalogName, tableIdentifier, "ALL")
              .tableMetadata()
              .snapshots();
      assertThat(allSnapshots).hasSize(2);

      var refsSnapshots =
          catalogApi
              .loadTable(currentCatalogName, tableIdentifier, "REFS")
              .tableMetadata()
              .snapshots();
      assertThat(refsSnapshots).hasSize(1);
      assertThat(refsSnapshots.getFirst().snapshotId()).isEqualTo(snapshotIdA);
    } finally {
      genericTableApi.purge(currentCatalogName, namespace);
    }
  }

  @Test
  public void testCreateGenericTableWithReservedProperty() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl1");

    String ns = RESTUtil.encodeNamespace(tableIdentifier.namespace());
    try (Response res =
        genericTableApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/generic-tables/",
                Map.of("cat", currentCatalogName, "ns", ns))
            .post(
                Entity.json(
                    CreateGenericTableRequest.builder()
                        .setName(tableIdentifier.name())
                        .setFormat("format")
                        .setDoc("doc")
                        .setProperties(Map.of("polaris.reserved", "true"))
                        .build()))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains("reserved prefix");
    }

    genericTableApi.purge(currentCatalogName, namespace);
  }

  @Test
  public void testCreateNamespaceWithReservedProperty() {
    Namespace namespace = Namespace.of("ns1");
    assertThatCode(
            () -> {
              restCatalog.createNamespace(namespace, ImmutableMap.of("polaris.reserved", "true"));
            })
        .isInstanceOf(org.apache.iceberg.exceptions.BadRequestException.class)
        .hasMessageContaining("reserved prefix");
  }

  @Test
  public void testUpdateNamespaceWithReservedProperty() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace, ImmutableMap.of("a", "b"));
    restCatalog.setProperties(namespace, ImmutableMap.of("c", "d"));
    Assertions.assertThatCode(
            () -> {
              restCatalog.setProperties(namespace, ImmutableMap.of("polaris.reserved", "true"));
            })
        .isInstanceOf(org.apache.iceberg.exceptions.BadRequestException.class)
        .hasMessageContaining("reserved prefix");
    genericTableApi.purge(currentCatalogName, namespace);
  }

  @Test
  public void testRemoveReservedPropertyFromNamespace() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace, ImmutableMap.of("a", "b"));
    restCatalog.removeProperties(namespace, Sets.newHashSet("a"));
    Assertions.assertThatCode(
            () -> {
              restCatalog.removeProperties(namespace, Sets.newHashSet("polaris.reserved"));
            })
        .isInstanceOf(org.apache.iceberg.exceptions.BadRequestException.class)
        .hasMessageContaining("reserved prefix");
    genericTableApi.purge(currentCatalogName, namespace);
  }

  @Test
  public void testCreateTableWithReservedProperty() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(namespace, "t1");
    Assertions.assertThatCode(
            () -> {
              restCatalog.createTable(
                  identifier,
                  SCHEMA,
                  PartitionSpec.unpartitioned(),
                  ImmutableMap.of("polaris.reserved", ""));
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved prefix");
    genericTableApi.purge(currentCatalogName, namespace);
  }

  @Test
  public void testUpdateTableWithReservedProperty() {
    Namespace namespace = Namespace.of("ns1");
    restCatalog.createNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(namespace, "t1");
    restCatalog.createTable(identifier, SCHEMA);
    Assertions.assertThatCode(
            () -> {
              var txn =
                  restCatalog.newReplaceTableTransaction(
                      identifier,
                      SCHEMA,
                      PartitionSpec.unpartitioned(),
                      ImmutableMap.of("polaris.reserved", ""),
                      false);
              txn.commitTransaction();
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved prefix");
    genericTableApi.purge(currentCatalogName, namespace);
  }

  @Test
  public void testLoadTableWithNonMatchingIfNoneMatchHeader() {
    // Create a table first
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    restCatalog
        .buildTable(
            TableIdentifier.of(ns1, "test_table"),
            new Schema(List.of(Types.NestedField.required(1, "col1", Types.StringType.get()))))
        .create();

    // Load table with a non-matching If-None-Match header
    String nonMatchingETag = "W/\"non-matching-etag-value\"";
    Invocation invocation =
        catalogApi
            .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/test_table")
            .header(HttpHeaders.IF_NONE_MATCH, nonMatchingETag)
            .build("GET");

    try (Response response = invocation.invoke()) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(response.getHeaders()).containsKey(HttpHeaders.ETAG);

      LoadTableResponse loadTableResponse = response.readEntity(LoadTableResponse.class);
      assertThat(loadTableResponse).isNotNull();
      assertThat(loadTableResponse.metadataLocation()).isNotNull();
    }
  }

  @Test
  public void testLoadTableWithMultipleIfNoneMatchETags() {
    // Create a table first
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    restCatalog
        .buildTable(
            TableIdentifier.of(ns1, "test_table"),
            new Schema(List.of(Types.NestedField.required(1, "col1", Types.StringType.get()))))
        .create();

    // First, load the table to get the ETag
    Invocation initialInvocation =
        catalogApi
            .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/test_table")
            .build("GET");

    String correctETag;
    try (Response initialResponse = initialInvocation.invoke()) {
      assertThat(initialResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(initialResponse.getHeaders()).containsKey(HttpHeaders.ETAG);
      correctETag = initialResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();
    }

    // Create multiple ETags, one of which matches
    String wrongETag1 = "W/\"wrong-etag-1\"";
    String wrongETag2 = "W/\"wrong-etag-2\"";
    String multipleETags = wrongETag1 + ", " + correctETag + ", " + wrongETag2;

    // Load the table with multiple ETags
    Invocation etaggedInvocation =
        catalogApi
            .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/test_table")
            .header(HttpHeaders.IF_NONE_MATCH, multipleETags)
            .build("GET");

    try (Response etaggedResponse = etaggedInvocation.invoke()) {
      assertThat(etaggedResponse.getStatus())
          .isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
      assertThat(etaggedResponse.hasEntity()).isFalse();
    }
  }

  @Test
  public void testLoadTableWithWildcardIfNoneMatchReturns400() {
    // Create a table first
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    restCatalog
        .buildTable(
            TableIdentifier.of(ns1, "test_table"),
            new Schema(List.of(Types.NestedField.required(1, "col1", Types.StringType.get()))))
        .create();

    // Load table with wildcard If-None-Match header (should be rejected)
    Invocation invocation =
        catalogApi
            .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/test_table")
            .header(HttpHeaders.IF_NONE_MATCH, "*")
            .build("GET");

    try (Response response = invocation.invoke()) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }
  }

  @Test
  public void testLoadNonExistentTableWithIfNoneMatch() {
    // Create namespace but not the table
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);

    // Try to load a non-existent table with If-None-Match header
    String etag = "W/\"some-etag\"";
    Invocation invocation =
        catalogApi
            .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/non_existent_table")
            .header(HttpHeaders.IF_NONE_MATCH, etag)
            .build("GET");

    try (Response response = invocation.invoke()) {
      // Should return 404 Not Found regardless of If-None-Match header
      assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }
  }

  @Test
  public void testETagBehaviorForTableSchemaChanges() {
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableIdentifier tableId = TableIdentifier.of(ns1, "test_schema_evolution_table");

    // Create initial table with v1 schema
    Schema v1Schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())));
    restCatalog.buildTable(tableId, v1Schema).create();

    // Load table and get v1 ETag
    Invocation v1Invocation =
        catalogApi
            .request(
                "v1/" + currentCatalogName + "/namespaces/ns1/tables/test_schema_evolution_table")
            .build("GET");

    String v1ETag;
    try (Response v1Response = v1Invocation.invoke()) {
      assertThat(v1Response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(v1Response.getHeaders()).containsKey(HttpHeaders.ETAG);
      v1ETag = v1Response.getHeaders().getFirst(HttpHeaders.ETAG).toString();
    }

    // Evolve schema to v2 (add email column)
    restCatalog
        .loadTable(tableId)
        .updateSchema()
        .addColumn("email", Types.StringType.get())
        .commit();

    // Load table and get v2 ETag
    Invocation v2Invocation =
        catalogApi
            .request(
                "v1/" + currentCatalogName + "/namespaces/ns1/tables/test_schema_evolution_table")
            .build("GET");

    String v2ETag;
    try (Response v2Response = v2Invocation.invoke()) {
      assertThat(v2Response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(v2Response.getHeaders()).containsKey(HttpHeaders.ETAG);
      v2ETag = v2Response.getHeaders().getFirst(HttpHeaders.ETAG).toString();
    }

    // Evolve schema to v3 (add age column)
    restCatalog
        .loadTable(tableId)
        .updateSchema()
        .addColumn("age", Types.IntegerType.get())
        .commit();

    // Load table and get v3 ETag
    Invocation v3Invocation =
        catalogApi
            .request(
                "v1/" + currentCatalogName + "/namespaces/ns1/tables/test_schema_evolution_table")
            .build("GET");

    String v3ETag;
    try (Response v3Response = v3Invocation.invoke()) {
      assertThat(v3Response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(v3Response.getHeaders()).containsKey(HttpHeaders.ETAG);
      v3ETag = v3Response.getHeaders().getFirst(HttpHeaders.ETAG).toString();
    }

    // Verify all ETags are different
    assertThat(v1ETag).isNotEqualTo(v2ETag);
    assertThat(v1ETag).isNotEqualTo(v3ETag);
    assertThat(v2ETag).isNotEqualTo(v3ETag);

    // Test If-None-Match with v1 ETag against current v3 table
    Invocation v1EtagTestInvocation =
        catalogApi
            .request(
                "v1/" + currentCatalogName + "/namespaces/ns1/tables/test_schema_evolution_table")
            .header(HttpHeaders.IF_NONE_MATCH, v1ETag)
            .build("GET");

    try (Response v1EtagTestResponse = v1EtagTestInvocation.invoke()) {
      // Should return 200 OK because table has evolved since v1
      assertThat(v1EtagTestResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(v1EtagTestResponse.getHeaders()).containsKey(HttpHeaders.ETAG);

      String currentETag = v1EtagTestResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();
      assertThat(currentETag).isEqualTo(v3ETag); // Should match current v3 ETag
    }

    // Test with multiple ETags including v1 and v2
    String multipleETags = v1ETag + ", " + v2ETag;
    Invocation multipleEtagsInvocation =
        catalogApi
            .request(
                "v1/" + currentCatalogName + "/namespaces/ns1/tables/test_schema_evolution_table")
            .header(HttpHeaders.IF_NONE_MATCH, multipleETags)
            .build("GET");

    try (Response multipleEtagsResponse = multipleEtagsInvocation.invoke()) {
      // Should return 200 OK because current v3 ETag doesn't match v1 or v2
      assertThat(multipleEtagsResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Test with multiple ETags including v1 and v3
    multipleETags = v1ETag + ", " + v3ETag;
    multipleEtagsInvocation =
        catalogApi
            .request(
                "v1/" + currentCatalogName + "/namespaces/ns1/tables/test_schema_evolution_table")
            .header(HttpHeaders.IF_NONE_MATCH, multipleETags)
            .build("GET");

    try (Response multipleEtagsResponse = multipleEtagsInvocation.invoke()) {
      // Should return 304 Not Modified because ETag matches current v3
      assertThat(multipleEtagsResponse.getStatus())
          .isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
      assertThat(multipleEtagsResponse.hasEntity()).isFalse();
    }

    // Test with current v3 ETag
    Invocation currentEtagInvocation =
        catalogApi
            .request(
                "v1/" + currentCatalogName + "/namespaces/ns1/tables/test_schema_evolution_table")
            .header(HttpHeaders.IF_NONE_MATCH, v3ETag)
            .build("GET");

    try (Response currentEtagResponse = currentEtagInvocation.invoke()) {
      // Should return 304 Not Modified because ETag matches current version
      assertThat(currentEtagResponse.getStatus())
          .isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
      assertThat(currentEtagResponse.hasEntity()).isFalse();
    }
  }

  @Test
  public void testETagBehaviorForTableDropAndRecreateIntegration() {
    // Integration test equivalent of testETagBehaviorForTableDropAndRecreate unit test
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableIdentifier tableId = TableIdentifier.of(ns1, "test_drop_recreate_behavior_table");

    // Create original table
    Schema originalSchema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "original_id", Types.LongType.get()),
                Types.NestedField.optional(2, "original_name", Types.StringType.get())));
    restCatalog.buildTable(tableId, originalSchema).create();

    // Load original table and get ETag
    Invocation originalInvocation =
        catalogApi
            .request(
                "v1/"
                    + currentCatalogName
                    + "/namespaces/ns1/tables/test_drop_recreate_behavior_table")
            .build("GET");

    String originalETag;
    String originalMetadataLocation;
    try (Response originalResponse = originalInvocation.invoke()) {
      assertThat(originalResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(originalResponse.getHeaders()).containsKey(HttpHeaders.ETAG);
      originalETag = originalResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();

      LoadTableResponse originalLoadResponse = originalResponse.readEntity(LoadTableResponse.class);
      originalMetadataLocation = originalLoadResponse.metadataLocation();
    }

    // Drop the table
    restCatalog.dropTable(tableId);

    // Recreate table with completely different schema
    Schema recreatedSchema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "recreated_uuid", Types.StringType.get()),
                Types.NestedField.optional(2, "recreated_data", Types.StringType.get()),
                Types.NestedField.optional(
                    3, "recreated_timestamp", Types.TimestampType.withoutZone())));
    restCatalog.buildTable(tableId, recreatedSchema).create();

    // Load recreated table and get ETag
    Invocation recreatedInvocation =
        catalogApi
            .request(
                "v1/"
                    + currentCatalogName
                    + "/namespaces/ns1/tables/test_drop_recreate_behavior_table")
            .build("GET");

    String recreatedETag;
    String recreatedMetadataLocation;
    try (Response recreatedResponse = recreatedInvocation.invoke()) {
      assertThat(recreatedResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(recreatedResponse.getHeaders()).containsKey(HttpHeaders.ETAG);
      recreatedETag = recreatedResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();

      LoadTableResponse recreatedLoadResponse =
          recreatedResponse.readEntity(LoadTableResponse.class);
      recreatedMetadataLocation = recreatedLoadResponse.metadataLocation();
    }

    // Verify ETags and metadata locations are completely different
    assertThat(originalETag).isNotEqualTo(recreatedETag);
    assertThat(originalMetadataLocation).isNotEqualTo(recreatedMetadataLocation);

    // Test If-None-Match with original ETag against recreated table
    Invocation originalEtagTestInvocation =
        catalogApi
            .request(
                "v1/"
                    + currentCatalogName
                    + "/namespaces/ns1/tables/test_drop_recreate_behavior_table")
            .header(HttpHeaders.IF_NONE_MATCH, originalETag)
            .build("GET");

    try (Response originalEtagTestResponse = originalEtagTestInvocation.invoke()) {
      // Should return 200 OK because it's a completely different table
      assertThat(originalEtagTestResponse.getStatus())
          .isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(originalEtagTestResponse.getHeaders()).containsKey(HttpHeaders.ETAG);

      String currentETag =
          originalEtagTestResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();
      assertThat(currentETag).isEqualTo(recreatedETag); // Should match recreated table ETag

      LoadTableResponse currentLoadResponse =
          originalEtagTestResponse.readEntity(LoadTableResponse.class);

      // Verify we get the recreated table schema (not the original)
      assertThat(currentLoadResponse.tableMetadata().schema().columns()).hasSize(3);
      assertThat(currentLoadResponse.tableMetadata().schema().findField("recreated_uuid"))
          .isNotNull();
      assertThat(currentLoadResponse.tableMetadata().schema().findField("recreated_data"))
          .isNotNull();
      assertThat(currentLoadResponse.tableMetadata().schema().findField("recreated_timestamp"))
          .isNotNull();

      // Verify original schema fields are NOT present
      assertThat(currentLoadResponse.tableMetadata().schema().findField("original_id")).isNull();
      assertThat(currentLoadResponse.tableMetadata().schema().findField("original_name")).isNull();
    }

    // Test with current recreated ETag
    Invocation currentEtagInvocation =
        catalogApi
            .request(
                "v1/"
                    + currentCatalogName
                    + "/namespaces/ns1/tables/test_drop_recreate_behavior_table")
            .header(HttpHeaders.IF_NONE_MATCH, recreatedETag)
            .build("GET");

    try (Response currentEtagResponse = currentEtagInvocation.invoke()) {
      // Should return 304 Not Modified because ETag matches current recreated table
      assertThat(currentEtagResponse.getStatus())
          .isEqualTo(Response.Status.NOT_MODIFIED.getStatusCode());
      assertThat(currentEtagResponse.hasEntity()).isFalse();
    }
  }

  @Test
  public void testETagChangeAfterDMLOperations() {
    // Test that ETags change after DML operations (INSERT, UPDATE, DELETE)
    Namespace ns1 = Namespace.of("ns1");
    restCatalog.createNamespace(ns1);
    TableIdentifier tableId = TableIdentifier.of(ns1, "test_dml_etag_table");

    // Create table with initial schema
    Schema schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "value", Types.IntegerType.get())));
    restCatalog.buildTable(tableId, schema).create();

    // Load table and get initial ETag (before any data)
    Invocation initialInvocation =
        catalogApi
            .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/test_dml_etag_table")
            .build("GET");

    String initialETag;
    String initialMetadataLocation;
    try (Response initialResponse = initialInvocation.invoke()) {
      assertThat(initialResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(initialResponse.getHeaders()).containsKey(HttpHeaders.ETAG);
      initialETag = initialResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();

      LoadTableResponse initialLoadResponse = initialResponse.readEntity(LoadTableResponse.class);
      initialMetadataLocation = initialLoadResponse.metadataLocation();
    }

    // Simulate DML operation by creating a new snapshot (append operation)
    Table table = restCatalog.loadTable(tableId);

    // Create a data file and append it (simulating INSERT operation)
    AppendFiles append = table.newAppend();

    // Create a mock data file entry
    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath(table.locationProvider().newDataLocation("file1.parquet"))
            .withFileSizeInBytes(1024)
            .withRecordCount(100)
            .build();

    append.appendFile(dataFile);
    append.commit(); // This creates a new snapshot and should change the ETag

    // Load table after DML operation and get new ETag
    Invocation afterDMLInvocation =
        catalogApi
            .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/test_dml_etag_table")
            .build("GET");

    String afterDMLETag;
    String afterDMLMetadataLocation;
    try (Response afterDMLResponse = afterDMLInvocation.invoke()) {
      assertThat(afterDMLResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(afterDMLResponse.getHeaders()).containsKey(HttpHeaders.ETAG);
      afterDMLETag = afterDMLResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();

      LoadTableResponse afterDMLLoadResponse = afterDMLResponse.readEntity(LoadTableResponse.class);
      afterDMLMetadataLocation = afterDMLLoadResponse.metadataLocation();
    }

    // Verify ETag and metadata location changed after DML operation
    assertThat(initialETag).isNotEqualTo(afterDMLETag);
    assertThat(initialMetadataLocation).isNotEqualTo(afterDMLMetadataLocation);

    // Test If-None-Match with initial ETag after DML operation
    Invocation initialEtagTestInvocation =
        catalogApi
            .request("v1/" + currentCatalogName + "/namespaces/ns1/tables/test_dml_etag_table")
            .header(HttpHeaders.IF_NONE_MATCH, initialETag)
            .build("GET");

    try (Response initialEtagTestResponse = initialEtagTestInvocation.invoke()) {
      // Should return 200 OK because table has new snapshot after DML
      assertThat(initialEtagTestResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(initialEtagTestResponse.getHeaders()).containsKey(HttpHeaders.ETAG);

      String currentETag =
          initialEtagTestResponse.getHeaders().getFirst(HttpHeaders.ETAG).toString();
      assertThat(currentETag).isEqualTo(afterDMLETag); // Should match post-DML ETag
    }
  }

  @Test
  public void testPaginatedListNamespaces() {
    String prefix = "testPaginatedListNamespaces";
    for (int i = 0; i < 20; i++) {
      Namespace namespace = Namespace.of(prefix + i);
      restCatalog.createNamespace(namespace);
    }

    try {
      Assertions.assertThat(catalogApi.listNamespaces(currentCatalogName, Namespace.empty()))
          .hasSize(20);
      for (var pageSize : List.of(1, 2, 3, 9, 10, 11, 19, 20, 21, 2000)) {
        int total = 0;
        String pageToken = null;
        do {
          ListNamespacesResponse response =
              catalogApi.listNamespaces(
                  currentCatalogName, Namespace.empty(), pageToken, String.valueOf(pageSize));
          Assertions.assertThat(response.namespaces().size()).isLessThanOrEqualTo(pageSize);
          total += response.namespaces().size();
          pageToken = response.nextPageToken();
        } while (pageToken != null);
        Assertions.assertThat(total)
            .as("Total paginated results for pageSize = " + pageSize)
            .isEqualTo(20);
      }
    } finally {
      for (int i = 0; i < 20; i++) {
        Namespace namespace = Namespace.of(prefix + i);
        restCatalog.dropNamespace(namespace);
      }
    }
  }

  @Test
  public void testPaginatedListTables() {
    String prefix = "testPaginatedListTables";
    Namespace namespace = Namespace.of(prefix);
    restCatalog.createNamespace(namespace);
    for (int i = 0; i < 20; i++) {
      restCatalog.createTable(TableIdentifier.of(namespace, prefix + i), SCHEMA);
    }

    try {
      Assertions.assertThat(catalogApi.listTables(currentCatalogName, namespace)).hasSize(20);
      for (var pageSize : List.of(1, 2, 3, 9, 10, 11, 19, 20, 21, 2000)) {
        int total = 0;
        String pageToken = null;
        do {
          ListTablesResponse response =
              catalogApi.listTables(
                  currentCatalogName, namespace, pageToken, String.valueOf(pageSize));
          Assertions.assertThat(response.identifiers().size()).isLessThanOrEqualTo(pageSize);
          total += response.identifiers().size();
          pageToken = response.nextPageToken();
        } while (pageToken != null);
        Assertions.assertThat(total)
            .as("Total paginated results for pageSize = " + pageSize)
            .isEqualTo(20);
      }
    } finally {
      for (int i = 0; i < 20; i++) {
        restCatalog.dropTable(TableIdentifier.of(namespace, prefix + i));
      }
      restCatalog.dropNamespace(namespace);
    }
  }
}
