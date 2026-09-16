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
package org.apache.polaris.service.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.GenericTableApi;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.PolicyApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * CDI-level coverage for {@link S3CredentialVendingMechanisms}: discovery through the real
 * container, and the standalone contract this module alone must satisfy: an allowlisted mechanism
 * with no bean installed is refused everywhere it would be used, never confused with DEFAULT or
 * STS, and never aborts startup, while the DEFAULT and STS beans are proven to still vend through
 * the real registry dispatch. No cloud calls: every catalog's table content goes through {@link
 * TestInMemoryFileIOFactory} (selected by {@code polaris.file-io.type=test-in-memory}).
 *
 * <p>The generic-table and policy routes are namespace-scoped: {@code
 * CatalogHandler.authorizeBasicNamespaceOperationOrThrow} resolves and checks the namespace's
 * existence, throwing {@code NoSuchNamespaceException} on a namespace that was never created,
 * before it calls {@code initializeCatalog()} (the method the mechanism gate lives in; see that
 * class's own javadoc: "{@code initializeCatalog}... Called after all {@code authorize...}
 * methods."). Since nothing can be created inside a catalog whose mechanism has no installed bean,
 * this class follows the same pattern {@link
 * org.apache.polaris.service.admin.S3CredentialVendingMechanismRoutesTest} already established:
 * populate the namespace and table while the catalog is still {@code STS}, then switch its storage
 * config to the uninstalled mechanism through an update, so the namespace-scoped routes resolve
 * past authorization and reach {@code initializeCatalog()}'s gate.
 *
 * <p>The "not available in this server" assertions target a mechanism identifier the server never
 * ships, {@code UNINSTALLED_MECHANISM}.
 *
 * <p>This class installs only the {@code DEFAULT} and {@code STS} mechanisms (the server's real,
 * shipped beans) and allowlists {@code UNINSTALLED_MECHANISM} without installing it. {@link
 * S3CredentialVendingMechanismThirdMechanismCdiTest} swaps in a test-only mechanism, under its own
 * profile, to prove the registry and the gates work for a mechanism the server itself does not
 * ship. Quarkus does not allow {@code @TestProfile} on a {@code @Nested} class, so that scenario
 * cannot share this file: it needs its own application instance, since {@code
 * getEnabledAlternatives()} is a profile-wide, one-instance setting, and this class asserts {@code
 * availableIds()} is exactly {@code {DEFAULT, STS}}.
 */
@QuarkusTest
@TestProfile(S3CredentialVendingMechanismCdiTest.Profile.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
class S3CredentialVendingMechanismCdiTest {

  private static final String UNINSTALLED_MECHANISM = "UNINSTALLED_MECHANISM";
  private static final String NOT_AVAILABLE_UNINSTALLED =
      "S3 credential vending mechanism UNINSTALLED_MECHANISM is not available in this server";

  /** A second realm, used only to run the STS-vending check with the skip flag turned off. */
  private static final String NO_SKIP_REALM = "POLARIS2";

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.ofEntries(
          Map.entry("polaris.realm-context.realms", "POLARIS," + NO_SKIP_REALM),
          Map.entry("polaris.file-io.type", "test-in-memory"),
          Map.entry("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"S3\"]"),
          Map.entry("polaris.features.\"ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES\"", "true"),
          Map.entry("polaris.features.\"ENABLE_GENERIC_TABLES\"", "true"),
          Map.entry("polaris.features.\"ENABLE_POLICY_STORE\"", "true"),
          Map.entry("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "true"),
          Map.entry(
              "polaris.features.realm-overrides.\""
                  + NO_SKIP_REALM
                  + "\".\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"",
              "false"),
          Map.entry(
              "polaris.features.\"SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS\"",
              "[\"STS\",\"" + UNINSTALLED_MECHANISM + "\"]"),
          Map.entry("polaris.event-listener.type", "test"),
          Map.entry("polaris.authentication.token-broker.type", "symmetric-key"),
          Map.entry("polaris.authentication.token-broker.symmetric-key.secret", "secret"));
    }
  }

  @Inject S3CredentialVendingMechanisms mechanisms;

  /**
   * With the default readiness settings (no {@code polaris.readiness.ignore-severe-issues}
   * override) and one mechanism allowlisted but not installed, the application starts, only {@code
   * STS} is available, and every route that would open an uninstalled-mechanism catalog or vend for
   * it refuses with "not available in this server": the Iceberg, generic-table and policy routes
   * alike, and a storage-access resolution under {@code SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION}
   * too. An STS catalog in the same realm is untouched throughout.
   */
  @Test
  void stsOnlyDiscoveryAndUninstalledMechanismsAreRefusedEverywhere(
      PolarisApiEndpoints endpoints, ClientCredentials credentials) throws Exception {
    // The application started at all, with two mechanisms allowlisted and neither installed, and
    // default readiness settings, is itself part of what this proves; availableIds() shows only
    // STS installed.
    assertThat(mechanisms.availableIds()).containsExactly("DEFAULT", "STS");

    try (PolarisClient client = PolarisClient.polarisClient(endpoints)) {
      String adminToken = client.obtainToken(credentials);
      ManagementApi managementApi = client.managementApi(adminToken);
      CatalogApi catalogApi = client.catalogApi(adminToken);
      GenericTableApi genericTableApi = client.genericTableApi(adminToken);
      PolicyApi policyApi = client.policyApi(adminToken);

      String stsCatalog = "cdi-sts-cat";
      String uninstalledCatalog = "cdi-uninstalled-cat";

      // The STS catalog: content created normally, left untouched for the rest of the test.
      createStsCatalog(managementApi, stsCatalog);
      try (Response r =
          managementApi.request("v1/catalogs/{name}", Map.of("name", stsCatalog)).get()) {
        assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        assertThat(r.readEntity(String.class)).doesNotContain("credentialVendingMechanism");
      }
      catalogApi.createNamespace(stsCatalog, "ns");
      createTable(catalogApi, stsCatalog, "ns", "t");

      // The namespace-scoped routes (generic-tables, policies, loadTable) resolve and check the
      // namespace's existence before initializeCatalog() runs, so nothing gates them on a catalog
      // that never had a namespace. Populate this one as STS, then switch it to a mechanism the
      // server never ships, so its namespace and table already exist when the gate is exercised.
      createStsCatalog(managementApi, uninstalledCatalog);
      catalogApi.createNamespace(uninstalledCatalog, "ns");
      createTable(catalogApi, uninstalledCatalog, "ns", "t");
      switchToUninstalledMechanism(managementApi, uninstalledCatalog);

      assertRefused(
          catalogApi.request("v1/{cat}/namespaces", Map.of("cat", uninstalledCatalog)).get(),
          NOT_AVAILABLE_UNINSTALLED);
      assertRefused(
          genericTableApi
              .request(
                  "polaris/v1/{cat}/namespaces/{ns}/generic-tables",
                  Map.of("cat", uninstalledCatalog, "ns", "ns"))
              .get(),
          NOT_AVAILABLE_UNINSTALLED);
      assertRefused(
          policyApi
              .request(
                  "polaris/v1/{cat}/namespaces/{ns}/policies",
                  Map.of("cat", uninstalledCatalog, "ns", "ns"))
              .get(),
          NOT_AVAILABLE_UNINSTALLED);

      // SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION is on in this realm: the STS catalog's loadTable
      // still succeeds, while the uninstalled-mechanism catalog's loadTable stays refused. Which
      // of the two gates in that path fires first is proven separately by
      // StorageAccessConfigProviderMechanismGateTest; this assertion only shows the route's
      // outcome.
      try (Response ok =
          catalogApi
              .request(
                  "v1/{cat}/namespaces/{ns}/tables/{table}",
                  Map.of("cat", stsCatalog, "ns", "ns", "table", "t"))
              .get()) {
        assertThat(ok.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      }
      assertRefused(
          catalogApi
              .request(
                  "v1/{cat}/namespaces/{ns}/tables/{table}",
                  Map.of("cat", uninstalledCatalog, "ns", "ns", "table", "t"))
              .get(),
          NOT_AVAILABLE_UNINSTALLED);
    }
  }

  /**
   * Proves the second half of STS discovery that the other test method cannot: that the DEFAULT and
   * STS beans actually vend through the registry, not merely that the gate admits them. That test
   * method's realm has {@code SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION} on, so {@code
   * StorageAccessConfigProvider} returns before ever calling the registry; its 200s show only that
   * the route works. This method runs in a realm with the skip flag off, on catalogs with {@code
   * stsUnavailable: true}, so no real STS call is made: {@code
   * buildLoadTableResponseWithDelegationCredentials} calls {@code StorageAccessConfigProvider}
   * unconditionally on every load, delegation requested or not, which reaches the registry, which
   * resolves the catalog's mechanism (an absent value resolving to {@code @Identifier("DEFAULT")},
   * an explicit one to {@code @Identifier("STS")}) and runs {@code
   * AwsCredentialsStorageIntegration}, which checks {@code stsUnavailable}, skips the AssumeRole
   * call entirely, and returns only the catalog's non-credential storage properties (its region and
   * endpoint). A load succeeding with those properties present and no access key anywhere in the
   * response is only possible if the registry actually resolved the bean and ran its integration.
   * The sequence runs once for each mechanism, on its own catalog.
   */
  @Test
  void theDefaultAndStsMechanismsVendThroughTheRegistryWithoutARealStsCall(
      PolarisApiEndpoints endpoints, ClientCredentials credentials) throws Exception {
    try (PolarisClient client = PolarisClient.polarisClient(endpoints)) {
      String adminToken = client.obtainToken(credentials);
      ManagementApi managementApi = client.managementApi(adminToken);
      CatalogApi catalogApi = client.catalogApi(adminToken);
      Map<String, String> realmHeaders =
          Map.of("Authorization", "Bearer " + adminToken, "Polaris-Realm", NO_SKIP_REALM);

      String endpoint = "https://s3.example-compatible-store.test";
      String region = "us-east-1";

      Map<String, String> scenarios = new LinkedHashMap<>();
      scenarios.put("cdi-default-unavailable-cat", null);
      scenarios.put("cdi-sts-unavailable-cat", "STS");

      for (Map.Entry<String, String> scenario : scenarios.entrySet()) {
        String catalog = scenario.getKey();
        String mechanism = scenario.getValue();

        createStsUnavailableCatalog(
            managementApi, realmHeaders, catalog, mechanism, endpoint, region);

        // The catalog's auto-granted catalog_admin role carries CATALOG_MANAGE_ACCESS and
        // CATALOG_MANAGE_METADATA only. This grant broadens it to CATALOG_MANAGE_CONTENT, the same
        // grant ManagementApi.makeAdmin gives a caller-supplied catalog role.
        try (Response r =
            managementApi
                .request(
                    "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                    Map.of(
                        "cat", catalog, "role", PolarisEntityConstants.getNameOfCatalogAdminRole()),
                    Map.of(),
                    realmHeaders)
                .put(
                    Entity.json(
                        new CatalogGrant(
                            CatalogPrivilege.CATALOG_MANAGE_CONTENT,
                            GrantResource.TypeEnum.CATALOG)))) {
          assertThat(r.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
        }

        try (Response r =
            catalogApi
                .request("v1/{cat}/namespaces", Map.of("cat", catalog), Map.of(), realmHeaders)
                .post(
                    Entity.json(
                        CreateNamespaceRequest.builder()
                            .withNamespace(Namespace.of("ns"))
                            .build()))) {
          assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        }

        try (Response r =
            catalogApi
                .request(
                    "v1/{cat}/namespaces/{ns}/tables",
                    Map.of("cat", catalog, "ns", "ns"),
                    Map.of(),
                    realmHeaders)
                .post(
                    Entity.json(
                        CreateTableRequest.builder()
                            .withName("t")
                            .withSchema(PolarisAuthzTestBase.SCHEMA)
                            .withLocation("s3://bucket/base/" + catalog + "/ns/t/")
                            .build()))) {
          assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        }

        // No X-Iceberg-Access-Delegation header: buildLoadTableResponseWithDelegationCredentials
        // calls StorageAccessConfigProvider unconditionally for every loadTable, delegation
        // requested or not, so this still reaches the registry and the resolved bean's
        // integration. Requesting delegation here does not work hermetically: a single
        // "vended-credentials" mode with no credentials available throws ("Credential vending
        // was requested... but no credentials are available"), and requesting both modes
        // together resolves to remote-signing once STS is unavailable, which
        // IcebergCatalogHandler rejects outright as not yet implemented. A plain load avoids
        // both and still proves the same thing: the registry resolved the bean and ran its
        // integration to produce these properties.
        try (Response r =
            catalogApi
                .request(
                    "v1/{cat}/namespaces/{ns}/tables/{table}",
                    Map.of("cat", catalog, "ns", "ns", "table", "t"),
                    Map.of(),
                    realmHeaders)
                .get()) {
          assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
          LoadTableResponse loaded = r.readEntity(LoadTableResponse.class);
          assertThat(loaded.credentials()).isEmpty();
          assertThat(loaded.config())
              .containsEntry(StorageAccessProperty.CLIENT_REGION.getPropertyName(), region)
              .containsEntry(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), endpoint)
              .doesNotContainKey(StorageAccessProperty.AWS_KEY_ID.getPropertyName());
        }
      }
    }
  }

  private static void createStsUnavailableCatalog(
      ManagementApi managementApi,
      Map<String, String> realmHeaders,
      String name,
      @Nullable String mechanism,
      String endpoint,
      String region) {
    AwsStorageConfigInfo.Builder storageConfig =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/r")
            .setStsUnavailable(true)
            .setEndpoint(endpoint)
            .setPathStyleAccess(true)
            .setRegion(region)
            .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"));
    if (mechanism != null) {
      storageConfig.setCredentialVendingMechanism(mechanism);
    }
    try (Response r =
        managementApi
            .request("v1/catalogs", Map.of(), Map.of(), realmHeaders)
            .post(
                Entity.json(
                    new CreateCatalogRequest(
                        PolarisCatalog.builder()
                            .setType(Catalog.TypeEnum.INTERNAL)
                            .setName(name)
                            .setProperties(new CatalogProperties("s3://bucket/base/" + name))
                            .setStorageConfigInfo(storageConfig.build())
                            .build())))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  private static void createStsCatalog(ManagementApi managementApi, String name) {
    managementApi.createCatalog(
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(name)
            .setProperties(new CatalogProperties("s3://bucket/base/" + name))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                    .setRoleArn("arn:aws:iam::123456789012:role/r")
                    .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                    .build())
            .build());
  }

  /**
   * Switches an existing STS catalog's storage config to a mechanism the server never ships,
   * freezing its content. {@link ManagementApi#updateCatalog(Catalog, Map)} always reuses the
   * existing storage config, so the switch needs a raw request carrying the new one.
   */
  private static void switchToUninstalledMechanism(ManagementApi managementApi, String name) {
    Catalog fetched = managementApi.getCatalog(name);
    UpdateCatalogRequest toUninstalled =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/base/" + name),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setCredentialVendingMechanism(UNINSTALLED_MECHANISM)
                .setRoleArn("arn:aws:iam::123456789012:role/r")
                .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                .build());
    try (Response r =
        managementApi
            .request("v1/catalogs/{name}", Map.of("name", name))
            .put(Entity.json(toUninstalled))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private static void createTable(CatalogApi catalogApi, String catalog, String ns, String table) {
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName(table)
            .withSchema(PolarisAuthzTestBase.SCHEMA)
            .withLocation("s3://bucket/base/" + catalog + "/" + ns + "/" + table + "/")
            .build();
    try (Response r =
        catalogApi
            .request("v1/{cat}/namespaces/{ns}/tables", Map.of("cat", catalog, "ns", ns))
            .post(Entity.json(request))) {
      assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private static void assertRefused(Response response, String expectedMessage) {
    try (response) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      ErrorResponse body = ErrorResponseParser.fromJson(response.readEntity(String.class));
      assertThat(body.message()).isEqualTo(expectedMessage);
    }
  }
}
