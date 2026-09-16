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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Every Iceberg route that opens a catalog whose selected mechanism has no installed bean is
 * refused at initialization, namespace reads included, with or without
 * SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION; a catalog with an empty mechanism in the same realm is
 * untouched. The catalog is created directly with {@link #TEST_MECHANISM}, a mechanism {@link
 * TestServices} installs through {@code additionalVendingMechanisms}, populated while that
 * mechanism is installed, then the "not available" scenario removes it from {@code
 * installedMechanisms()} afterwards, which is what a server rebuilt without the mechanism looks
 * like to a stored catalog. Every catalog gets its own allowed location: upstream rejects
 * overlapping catalog locations at create and update.
 *
 * <p>The policy routes go through {@code PolicyCatalogHandler}, which {@link TestServices} does not
 * wire (it has no policy API): building that handler by hand here would need the authorizer and
 * resolution internals {@link TestServices} keeps private to its {@code build()} closure. That case
 * is covered by {@link org.apache.polaris.service.storage.S3CredentialVendingMechanismCdiTest}.
 */
class S3CredentialVendingMechanismRoutesTest {

  private static final String TEST_MECHANISM = "TEST_MECHANISM";
  private static final String NOT_AVAILABLE =
      "S3 credential vending mechanism " + TEST_MECHANISM + " is not available in this server";
  private static final String NOT_ENABLED =
      "S3 credential vending mechanism " + TEST_MECHANISM + " is not enabled in this realm";

  /** A mutable config map: TestServices reads it live, so a test can flip the realm allowlist. */
  private static Map<String, Object> config(boolean skipSubscoping) {
    Map<String, Object> config = new HashMap<>();
    config.put("SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS", List.of("STS", TEST_MECHANISM));
    config.put("SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION", skipSubscoping);
    return config;
  }

  private static TestServices services(Map<String, Object> config) {
    return TestServices.builder()
        .config(config)
        .additionalVendingMechanisms(Map.of(TEST_MECHANISM, TestServices.fakeMechanism()))
        .fileIOFactorySupplier(
            () ->
                (FileIOFactory) (accessConfig, ioImplClassName, properties) -> new InMemoryFileIO())
        .build();
  }

  private static Catalog emptyMechanismCatalog(String name) {
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(name)
        .setProperties(new CatalogProperties("s3://bucket/base/" + name))
        .setStorageConfigInfo(
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setRoleArn("arn:aws:iam::123456789012:role/r")
                .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                .build())
        .build();
  }

  private static void createCatalog(TestServices svc, String name) {
    try (Response r =
        svc.catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(emptyMechanismCatalog(name)),
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  private static void createNamespace(TestServices svc, String catalog, String ns) {
    try (Response r =
        svc.restApi()
            .createNamespace(
                catalog,
                CreateNamespaceRequest.builder().withNamespace(Namespace.of(ns)).build(),
                null,
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  /** The table's location sits under the catalog's own allowed location. */
  private static void createTable(TestServices svc, String catalog, String ns, String table) {
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName(table)
            .withSchema(PolarisAuthzTestBase.SCHEMA)
            .withLocation("s3://bucket/base/" + catalog + "/" + ns + "/" + table + "/")
            .build();
    try (Response r =
        svc.restApi()
            .createTable(
                catalog, ns, request, null, null, svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private static void assertLoadTableRefused(
      TestServices svc,
      String catalog,
      String ns,
      String table,
      String accessDelegationMode,
      String expectedMessage) {
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .loadTable(
                        catalog,
                        ns,
                        table,
                        accessDelegationMode,
                        null,
                        "ALL",
                        null,
                        svc.realmContext(),
                        svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(expectedMessage);
  }

  private static void assertLoadTableSucceeds(
      TestServices svc, String catalog, String ns, String table) {
    try (Response r =
        svc.restApi()
            .loadTable(
                catalog,
                ns,
                table,
                null,
                null,
                "ALL",
                null,
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private static Catalog testMechanismCatalog(String name) {
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(name)
        .setProperties(new CatalogProperties("s3://bucket/base/" + name))
        .setStorageConfigInfo(
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setCredentialVendingMechanism(TEST_MECHANISM)
                .setRoleArn("arn:aws:iam::123456789012:role/r")
                .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                .build())
        .build();
  }

  private static void createTestMechanismCatalog(TestServices svc, String name) {
    try (Response r =
        svc.catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(testMechanismCatalog(name)),
                svc.realmContext(),
                svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void everyIcebergRouteOnAnUninstalledMechanismCatalogIsRefusedAtInitialization(
      boolean skipSubscoping) {
    TestServices svc = services(config(skipSubscoping));
    createTestMechanismCatalog(svc, "mechcat");
    createNamespace(svc, "mechcat", "ns");
    createTable(svc, "mechcat", "ns", "t");
    createCatalog(svc, "emptycat");
    createNamespace(svc, "emptycat", "ns");
    createTable(svc, "emptycat", "ns", "t");
    svc.installedMechanisms().remove(TEST_MECHANISM);

    assertThatThrownBy(
            () ->
                svc.restApi()
                    .listNamespaces(
                        "mechcat", null, null, null, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_AVAILABLE);
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .loadNamespaceMetadata(
                        "mechcat", "ns", svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_AVAILABLE);
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .createNamespace(
                        "mechcat",
                        CreateNamespaceRequest.builder().withNamespace(Namespace.of("ns2")).build(),
                        null,
                        svc.realmContext(),
                        svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_AVAILABLE);
    assertLoadTableRefused(svc, "mechcat", "ns", "t", "vended-credentials", NOT_AVAILABLE);
    assertLoadTableRefused(svc, "mechcat", "ns", "t", null, NOT_AVAILABLE);

    // Management reads are unaffected, and the catalog with an empty mechanism in the same realm
    // still serves.
    try (Response r =
        svc.catalogsApi().getCatalog("mechcat", svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    try (Response r =
        svc.restApi()
            .listNamespaces(
                "emptycat", null, null, null, svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    assertLoadTableSucceeds(svc, "emptycat", "ns", "t");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void theRealmKillSwitchRefusesTheCatalogWithNotEnabled(boolean skipSubscoping) {
    Map<String, Object> config = config(skipSubscoping);
    TestServices svc = services(config);
    createTestMechanismCatalog(svc, "mechkill");
    createNamespace(svc, "mechkill", "ns");
    createTable(svc, "mechkill", "ns", "t");
    createCatalog(svc, "emptykill");
    createNamespace(svc, "emptykill", "ns");
    createTable(svc, "emptykill", "ns", "t");

    // Engage the kill switch: the realm no longer lists TEST_MECHANISM. The mechanism itself
    // stays installed throughout.
    config.put("SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS", List.of("STS"));

    String notEnabled = NOT_ENABLED;
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .listNamespaces(
                        "mechkill", null, null, null, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(notEnabled);
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .loadNamespaceMetadata(
                        "mechkill", "ns", svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(notEnabled);
    assertLoadTableRefused(svc, "mechkill", "ns", "t", "vended-credentials", notEnabled);
    assertLoadTableRefused(svc, "mechkill", "ns", "t", null, notEnabled);
    // Management reads still work; an update carrying a storage config is refused at site 1.
    Catalog fetched;
    try (Response r =
        svc.catalogsApi().getCatalog("mechkill", svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      fetched = (Catalog) r.getEntity();
    }
    UpdateCatalogRequest touch =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/base/mechkill"),
            fetched.getStorageConfigInfo());
    assertThatThrownBy(
            () ->
                svc.catalogsApi()
                    .updateCatalog("mechkill", touch, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(notEnabled);
    // The catalog with an empty mechanism in the same realm is unaffected.
    try (Response r =
        svc.restApi()
            .listNamespaces(
                "emptykill", null, null, null, svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
    assertLoadTableSucceeds(svc, "emptykill", "ns", "t");
  }

  /**
   * An EXTERNAL catalog with an Iceberg REST connection and a storage config selecting {@link
   * #TEST_MECHANISM}, installed at create time.
   */
  private static void createExternalTestMechanismCatalog(TestServices svc, String name) {
    ConnectionConfigInfo connection =
        IcebergRestConnectionConfigInfo.builder(
                ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setUri("https://remote.example.com/api/catalog")
            .setRemoteCatalogName("remote")
            .setAuthenticationParameters(
                OAuthClientCredentialsParameters.builder(
                        AuthenticationParameters.AuthenticationTypeEnum.OAUTH)
                    .setClientId("client-id")
                    .setClientSecret("client-secret")
                    .setScopes(List.of("PRINCIPAL_ROLE:ALL"))
                    .build())
            .build();
    AwsStorageConfigInfo testMechanism =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism(TEST_MECHANISM)
            .setRoleArn("arn:aws:iam::123456789012:role/r")
            .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
            .build();
    Catalog external =
        ExternalCatalog.builder()
            .setType(ExternalCatalog.TypeEnum.EXTERNAL)
            .setName(name)
            .setProperties(new CatalogProperties("s3://bucket/base/" + name))
            .setStorageConfigInfo(testMechanism)
            .setConnectionConfigInfo(connection)
            .build();
    try (Response r =
        svc.catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(external), svc.realmContext(), svc.securityContext())) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  /**
   * An EXTERNAL catalog never reaches LocalIcebergCatalog, so the handler's own check is its only
   * gate. With the mechanism enabled but uninstalled the request stops at the gate with "not
   * available" (the federated factory lookup, which TestServices leaves unsatisfied, is never
   * reached); with the kill switch engaged it stops with "not enabled".
   */
  @Test
  void anUninstalledMechanismAndThenTheKillSwitchRefuseTheCatalog() {
    Map<String, Object> config = config(false);
    config.put("ENABLE_CATALOG_FEDERATION", true);
    TestServices svc = services(config);
    createExternalTestMechanismCatalog(svc, "mechext");
    svc.installedMechanisms().remove(TEST_MECHANISM);

    assertThatThrownBy(
            () ->
                svc.restApi()
                    .listNamespaces(
                        "mechext", null, null, null, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_AVAILABLE);

    config.put("SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS", List.of("STS"));
    assertThatThrownBy(
            () ->
                svc.restApi()
                    .listNamespaces(
                        "mechext", null, null, null, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_ENABLED);
  }

  /**
   * Generic-table routes open the catalog through their own handler; the gate applies there too.
   */
  @Test
  void anUninstalledMechanismAndThenTheKillSwitchRefuseGenericTableRoutes() {
    Map<String, Object> config = config(false);
    TestServices svc = services(config);
    createTestMechanismCatalog(svc, "mechgen");
    createNamespace(svc, "mechgen", "ns");
    svc.installedMechanisms().remove(TEST_MECHANISM);

    assertThatThrownBy(
            () ->
                svc.genericTableApi()
                    .listGenericTables(
                        "mechgen", "ns", null, null, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_AVAILABLE);

    config.put("SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS", List.of("STS"));
    assertThatThrownBy(
            () ->
                svc.genericTableApi()
                    .listGenericTables(
                        "mechgen", "ns", null, null, svc.realmContext(), svc.securityContext()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(NOT_ENABLED);
  }
}
