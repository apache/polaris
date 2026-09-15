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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.GenericTableApi;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.PolicyApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * CDI-level coverage for {@link S3CredentialVendingMechanisms}: discovery through the real
 * container, and the standalone contract PR 1 alone must satisfy — an allowlisted mechanism with no
 * bean installed is refused everywhere it would be used, never confused with STS, and never aborts
 * startup. No cloud calls: every catalog's table content goes through {@link
 * TestInMemoryFileIOFactory} (selected by {@code polaris.file-io.type=test-in-memory}), and {@code
 * CLOUDFLARE_R2} is exercised only as a storage-config value, never as a live endpoint.
 *
 * <p>The generic-table and policy routes are namespace-scoped: {@code
 * CatalogHandler.authorizeBasicNamespaceOperationOrThrow} resolves and checks the namespace's
 * existence, throwing {@code NoSuchNamespaceException} on a namespace that was never created,
 * before it calls {@code initializeCatalog()} — the method the mechanism gate lives in (see that
 * class's own javadoc: "{@code initializeCatalog}... Called after all {@code authorize...}
 * methods."). Since nothing can be created inside a {@code CLOUDFLARE_R2} catalog, this class
 * follows the same pattern the task 1/2 routes test already established: populate the namespace and
 * table while the catalog is still {@code STS}, then switch its storage config to {@code
 * CLOUDFLARE_R2} through an update, so the namespace-scoped routes resolve past authorization and
 * reach {@code initializeCatalog()}'s gate. A separately, directly created (never-populated) {@code
 * CLOUDFLARE_R2} catalog covers the plain create-and-201 case and the catalog-root namespace list,
 * which needs no pre-existing namespace since the root always resolves.
 *
 * <p>This class installs only the {@code STS} mechanism (the server's real, shipped bean) and
 * allowlists {@code CLOUDFLARE_R2} without installing it: the F1 contract from the design. {@link
 * S3CredentialVendingMechanismThirdMechanismCdiTest} swaps in a test-only third mechanism, under
 * its own profile, to prove the registry and the gates work for a mechanism the server itself does
 * not ship. Quarkus does not allow {@code @TestProfile} on a {@code @Nested} class, so the two
 * scenarios cannot share one file: each needs its own application instance, since {@code
 * getEnabledAlternatives()} is a profile-wide, one-instance setting and F1 requires {@code
 * availableIds()} to be exactly {@code {STS}}.
 */
@QuarkusTest
@TestProfile(S3CredentialVendingMechanismCdiTest.Profile.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
class S3CredentialVendingMechanismCdiTest {

  private static final String R2_ENDPOINT =
      "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com";
  private static final String NOT_AVAILABLE_R2 =
      "S3 credential vending mechanism CLOUDFLARE_R2 is not available in this server";

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.ofEntries(
          Map.entry("polaris.file-io.type", "test-in-memory"),
          Map.entry("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"S3\"]"),
          Map.entry("polaris.features.\"ALLOW_UNRESTRICTED_STORAGE_CONFIG_ROLE_CHANGES\"", "true"),
          Map.entry("polaris.features.\"ENABLE_GENERIC_TABLES\"", "true"),
          Map.entry("polaris.features.\"ENABLE_POLICY_STORE\"", "true"),
          Map.entry("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "true"),
          Map.entry(
              "polaris.features.\"SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS\"",
              "[\"STS\",\"CLOUDFLARE_R2\"]"),
          Map.entry("polaris.event-listener.type", "test"),
          Map.entry("polaris.authentication.token-broker.type", "symmetric-key"),
          Map.entry("polaris.authentication.token-broker.symmetric-key.secret", "secret"));
    }
  }

  @Inject S3CredentialVendingMechanisms mechanisms;

  /**
   * The F1 contract: with the default readiness settings (no {@code
   * polaris.readiness.ignore-severe-issues} override) and {@code CLOUDFLARE_R2} allowlisted but not
   * installed, the application starts, only {@code STS} is available, a {@code CLOUDFLARE_R2}
   * catalog is created and frozen, and every route that would open it or vend for it refuses with
   * "not available in this server" — the Iceberg, generic-table and policy routes alike, and a
   * storage-access resolution under {@code SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION} too, because the
   * gate runs before that early return. An STS catalog in the same realm is untouched throughout.
   */
  @Test
  void stsOnlyDiscoveryAndTheStandaloneCloudflareR2Contract(
      PolarisApiEndpoints endpoints, ClientCredentials credentials) throws Exception {
    // The application started at all, with the allowlist covering CLOUDFLARE_R2 and default
    // readiness settings, is itself part of the F1 proof; availableIds() shows only STS installed.
    assertThat(mechanisms.availableIds()).containsExactly("STS");

    try (PolarisClient client = PolarisClient.polarisClient(endpoints)) {
      String adminToken = client.obtainToken(credentials);
      ManagementApi managementApi = client.managementApi(adminToken);
      CatalogApi catalogApi = client.catalogApi(adminToken);
      GenericTableApi genericTableApi = client.genericTableApi(adminToken);
      PolicyApi policyApi = client.policyApi(adminToken);

      String stsCatalog = "cdi-sts-cat";
      String bareR2Catalog = "cdi-r2-bare-cat";
      String switchedR2Catalog = "cdi-r2-switched-cat";

      // The plain create-and-201 case, and the catalog-root namespace list: the root always
      // resolves at authorization even with nothing inside the catalog, so this needs no
      // pre-existing namespace.
      createCloudflareR2Catalog(managementApi, bareR2Catalog);
      assertRefused(
          catalogApi.request("v1/{cat}/namespaces", Map.of("cat", bareR2Catalog)).get(),
          NOT_AVAILABLE_R2);

      // The STS catalog: content created normally, left untouched for the rest of the test.
      createStsCatalog(managementApi, stsCatalog);
      catalogApi.createNamespace(stsCatalog, "ns");
      createTable(catalogApi, stsCatalog, "ns", "t");

      // The namespace-scoped routes (generic-tables, policies, loadTable) resolve and check the
      // namespace's existence before initializeCatalog() runs, so nothing gates them on a catalog
      // that never had a namespace. Populate this one as STS, then switch it to CLOUDFLARE_R2 so
      // its namespace and table already exist when the gate is exercised.
      createStsCatalog(managementApi, switchedR2Catalog);
      catalogApi.createNamespace(switchedR2Catalog, "ns");
      createTable(catalogApi, switchedR2Catalog, "ns", "t");
      switchToCloudflareR2(managementApi, switchedR2Catalog);

      assertRefused(
          genericTableApi
              .request(
                  "polaris/v1/{cat}/namespaces/{ns}/generic-tables",
                  Map.of("cat", switchedR2Catalog, "ns", "ns"))
              .get(),
          NOT_AVAILABLE_R2);
      assertRefused(
          policyApi
              .request(
                  "polaris/v1/{cat}/namespaces/{ns}/policies",
                  Map.of("cat", switchedR2Catalog, "ns", "ns"))
              .get(),
          NOT_AVAILABLE_R2);

      // SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION is on realm-wide: the STS catalog's loadTable still
      // succeeds (the skip short-circuits vending, not the route itself), while the switched
      // catalog's loadTable stays refused — the mechanism gate in StorageAccessConfigProvider runs
      // before the skip-subscoping early return.
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
                  Map.of("cat", switchedR2Catalog, "ns", "ns", "table", "t"))
              .get(),
          NOT_AVAILABLE_R2);
    }
  }

  private static void createStsCatalog(ManagementApi managementApi, String name) {
    managementApi.createCatalog(
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(name)
            .setProperties(
                new org.apache.polaris.core.admin.model.CatalogProperties(
                    "s3://bucket/base/" + name))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                    .setRoleArn("arn:aws:iam::123456789012:role/r")
                    .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                    .build())
            .build());
  }

  private static void createCloudflareR2Catalog(ManagementApi managementApi, String name) {
    managementApi.createCatalog(
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(name)
            .setProperties(
                new org.apache.polaris.core.admin.model.CatalogProperties(
                    "s3://bucket/base/" + name))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                    .setCredentialVendingMechanism(S3CredentialVendingMechanism.CLOUDFLARE_R2)
                    .setEndpoint(R2_ENDPOINT)
                    .setPathStyleAccess(true)
                    .setRegion("auto")
                    .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                    .build())
            .build());
  }

  /**
   * Switches an existing STS catalog's storage config to CLOUDFLARE_R2, freezing its content.
   * {@link ManagementApi#updateCatalog(Catalog, Map)} always reuses the existing storage config, so
   * the mechanism switch needs a raw request carrying the new one.
   */
  private static void switchToCloudflareR2(ManagementApi managementApi, String name) {
    Catalog fetched = managementApi.getCatalog(name);
    UpdateCatalogRequest toR2 =
        new UpdateCatalogRequest(
            fetched.getEntityVersion(),
            Map.of("default-base-location", "s3://bucket/base/" + name),
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setCredentialVendingMechanism(S3CredentialVendingMechanism.CLOUDFLARE_R2)
                .setEndpoint(R2_ENDPOINT)
                .setPathStyleAccess(true)
                .setRegion("auto")
                .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"))
                .build());
    try (Response r =
        managementApi.request("v1/catalogs/{name}", Map.of("name", name)).put(Entity.json(toR2))) {
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
