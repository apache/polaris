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
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
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
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A third {@code S3CredentialVendingMechanism} the server does not ship: proves the registry and
 * the gates generalize past {@code STS}. Runs under {@link ThirdMechanismProfile}, which enables
 * {@link TestS3CredentialVendingMechanism} as a CDI alternative and allowlists it for one realm but
 * not a second.
 *
 * <p>This is a separate top-level class, not a {@code @Nested} class inside {@link
 * S3CredentialVendingMechanismCdiTest}, because Quarkus rejects {@code @TestProfile} on
 * {@code @Nested} test classes ({@code io.quarkus.test.junit.QuarkusTestExtension}: "@Nested tests
 * may not contain @TestProfile annotations"), and the two scenarios need different application
 * instances regardless: {@code getEnabledAlternatives()} is profile-wide, and {@link
 * S3CredentialVendingMechanismCdiTest} asserts {@code availableIds()} is exactly {@code {STS}} in
 * its own application instance, which this test's third mechanism would otherwise widen.
 */
@QuarkusTest
@TestProfile(ThirdMechanismProfile.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
class S3CredentialVendingMechanismThirdMechanismCdiTest {

  @Inject S3CredentialVendingMechanisms mechanisms;

  @Inject
  @Identifier(TestS3CredentialVendingMechanism.ID)
  TestS3CredentialVendingMechanism testMechanism;

  @Test
  void anInstalledThirdMechanismVendsWhenAllowlistedAndIsRefusedWithoutDispatchWhenNot(
      PolarisApiEndpoints endpoints, ClientCredentials credentials) throws Exception {
    testMechanism.clear();
    assertThat(mechanisms.availableIds())
        .containsExactly("DEFAULT", "STS", TestS3CredentialVendingMechanism.ID);
    try (PolarisClient client = PolarisClient.polarisClient(endpoints)) {
      String adminToken = client.obtainToken(credentials);
      ManagementApi managementApi = client.managementApi(adminToken);
      CatalogApi catalogApi = client.catalogApi(adminToken);

      String catalog = "cdi-test-mech-cat";
      managementApi.createCatalog(testMechanismCatalog(catalog));
      assertThat(testMechanism.validations()).hasSize(1);
      assertThat(testMechanism.validations().get(0).current()).isNull();
      assertThat(testMechanism.validations().get(0).updated().getCredentialVendingMechanism())
          .isEqualTo(TestS3CredentialVendingMechanism.ID);

      // A config the mechanism itself refuses at validate() time: CatalogEntity accepts it (the
      // base location sits inside the one allowed location) and the refusal comes from validate().
      String refusedBase = "s3://bucket/base/refused/cdi-refused-cat";
      try (Response refused =
          managementApi
              .request("v1/catalogs")
              .post(
                  Entity.json(
                      new CreateCatalogRequest(
                          testMechanismCatalog("cdi-refused-cat", refusedBase))))) {
        assertRefused(refused, "TEST_MECHANISM refuses the allowed location " + refusedBase + "/");
      }

      // The catalog's auto-granted catalog_admin role (assigned to service_admin at creation)
      // carries CATALOG_MANAGE_ACCESS and CATALOG_MANAGE_METADATA only; loadTable with vended
      // credentials needs CATALOG_MANAGE_CONTENT too, the same grant ManagementApi.makeAdmin gives
      // a caller-supplied catalog role.
      managementApi.addGrant(
          catalog,
          PolarisEntityConstants.getNameOfCatalogAdminRole(),
          new CatalogGrant(
              CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG));
      catalogApi.createNamespace(catalog, "ns");
      createTable(catalogApi, catalog, "ns", "t");

      LoadTableResponse loaded =
          catalogApi.loadTableWithAccessDelegation(
              catalog, TableIdentifier.of(Namespace.of("ns"), "t"), null);
      assertThat(loaded.credentials()).hasSize(1);
      assertThat(loaded.credentials().get(0).config())
          .containsEntry(
              StorageAccessProperty.AWS_KEY_ID.getPropertyName(),
              TestS3CredentialVendingMechanism.FAKE_KEY)
          .containsEntry(
              StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(),
              TestS3CredentialVendingMechanism.FAKE_SECRET)
          .containsEntry(
              StorageAccessProperty.AWS_TOKEN.getPropertyName(),
              TestS3CredentialVendingMechanism.FAKE_TOKEN);
      // The end-to-end create-table-then-loadTable-with-delegation flow dispatches to the
      // mechanism more than once (once per storage action grouping FileIO and the credential
      // provider resolve internally); that fan-out is pre-existing StorageAccessConfigProvider
      // behaviour, unchanged by this feature, and orthogonal to what this test proves. What
      // matters here: the mechanism was reached at all, and every call it saw carried this
      // catalog's own config.
      assertThat(testMechanism.calls()).isNotEmpty();
      int callsAfterVending = testMechanism.calls().size();
      assertThat(testMechanism.calls())
          .allSatisfy(
              call -> {
                assertThat(call.storageConfig().getCredentialVendingMechanism())
                    .isEqualTo(TestS3CredentialVendingMechanism.ID);
                assertThat(call.storageConfig().getAllowedLocations())
                    .containsExactly("s3://bucket/base/" + catalog + "/");
              });

      // A realm where TEST_MECHANISM is not allowlisted: creation itself is refused with "not
      // enabled", at PolarisAdminService's allowlist-only gate, and the mechanism is never
      // dispatched, so the recorder gains no further call.
      Map<String, String> killSwitchRealmHeaders =
          Map.of(
              "Authorization",
              "Bearer " + adminToken,
              "Polaris-Realm",
              ThirdMechanismProfile.KILL_SWITCH_REALM);
      try (Response refused =
          managementApi
              .request("v1/catalogs", Map.of(), Map.of(), killSwitchRealmHeaders)
              .post(Entity.json(new CreateCatalogRequest(testMechanismCatalog("cdi-kill-cat"))))) {
        assertRefused(
            refused, "S3 credential vending mechanism TEST_MECHANISM is not enabled in this realm");
      }
      assertThat(testMechanism.calls()).hasSize(callsAfterVending);

      // Updating the first catalog with a second allowed location validates again, this time
      // with the stored config as current().
      Catalog fetched = managementApi.getCatalog(catalog);
      int validationsBeforeUpdate = testMechanism.validations().size();
      UpdateCatalogRequest addLocation =
          new UpdateCatalogRequest(
              fetched.getEntityVersion(),
              Map.of(),
              AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                  .setCredentialVendingMechanism(TestS3CredentialVendingMechanism.ID)
                  .setRoleArn("arn:aws:iam::123456789012:role/r")
                  .setAllowedLocations(
                      List.of(
                          "s3://bucket/base/" + catalog + "/",
                          "s3://bucket/base/" + catalog + "-extra/"))
                  .build());
      try (Response r =
          managementApi
              .request("v1/catalogs/{name}", Map.of("name", catalog))
              .put(Entity.json(addLocation))) {
        assertThat(r.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      }
      assertThat(testMechanism.validations()).hasSize(validationsBeforeUpdate + 1);
      assertThat(testMechanism.validations().get(validationsBeforeUpdate).current()).isNotNull();
    }
  }

  private static Catalog testMechanismCatalog(String name) {
    return testMechanismCatalog(name, "s3://bucket/base/" + name);
  }

  private static Catalog testMechanismCatalog(String name, String basePath) {
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(name)
        .setProperties(new org.apache.polaris.core.admin.model.CatalogProperties(basePath))
        .setStorageConfigInfo(
            AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                .setCredentialVendingMechanism(TestS3CredentialVendingMechanism.ID)
                .setRoleArn("arn:aws:iam::123456789012:role/r")
                .setAllowedLocations(List.of(basePath + "/"))
                .build())
        .build();
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
