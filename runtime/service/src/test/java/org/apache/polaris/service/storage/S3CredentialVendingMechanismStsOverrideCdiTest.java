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
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
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
 * Proves that a server's enabled alternative for the STS identifier serves both an empty {@code
 * credentialVendingMechanism} and an explicit {@code STS}. Runs under {@link
 * RecordingStsMechanismProfile}, which enables {@link RecordingStsCredentialVendingMechanism} as a
 * CDI alternative carrying {@code @Identifier(STS)}.
 */
@QuarkusTest
@TestProfile(RecordingStsMechanismProfile.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
class S3CredentialVendingMechanismStsOverrideCdiTest {

  @Inject S3CredentialVendingMechanisms mechanisms;

  @Inject
  @Identifier(S3CredentialVendingMechanism.STS)
  RecordingStsCredentialVendingMechanism override;

  @Test
  void anEmptyAndAnExplicitStsMechanismBothVendThroughTheEnabledAlternative(
      PolarisApiEndpoints endpoints, ClientCredentials credentials) throws Exception {
    override.clear();
    assertThat(mechanisms.availableIds()).containsExactly("STS");
    assertThat(mechanisms.require("STS"))
        .isInstanceOf(RecordingStsCredentialVendingMechanism.class);

    try (PolarisClient client = PolarisClient.polarisClient(endpoints)) {
      String adminToken = client.obtainToken(credentials);
      ManagementApi managementApi = client.managementApi(adminToken);
      CatalogApi catalogApi = client.catalogApi(adminToken);

      assertVendsThroughOverride(
          managementApi, catalogApi, "cdi-sts-override-empty-cat", /* explicitSts= */ false);
      assertVendsThroughOverride(
          managementApi, catalogApi, "cdi-sts-override-explicit-cat", /* explicitSts= */ true);
    }
  }

  private void assertVendsThroughOverride(
      ManagementApi managementApi, CatalogApi catalogApi, String catalog, boolean explicitSts) {
    override.clear();
    managementApi.createCatalog(mechanismCatalog(catalog, explicitSts));
    managementApi.addGrant(
        catalog,
        PolarisEntityConstants.getNameOfCatalogAdminRole(),
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG));
    catalogApi.createNamespace(catalog, "ns");
    createTable(catalogApi, catalog, "ns", "t");

    LoadTableResponse loaded =
        catalogApi.loadTableWithAccessDelegation(
            catalog, TableIdentifier.of(Namespace.of("ns"), "t"), null);
    assertThat(loaded.credentials()).hasSize(1);
    assertThat(loaded.credentials().get(0).config())
        .containsEntry(
            StorageAccessProperty.AWS_KEY_ID.getPropertyName(),
            RecordingStsCredentialVendingMechanism.FAKE_KEY_FOR_TEST);
    assertThat(override.calls()).isNotEmpty();
    assertThat(override.calls())
        .allSatisfy(
            call ->
                assertThat(call.storageConfig().getCredentialVendingMechanism())
                    .isEqualTo(explicitSts ? "STS" : null));
  }

  private static Catalog mechanismCatalog(String name, boolean explicitSts) {
    AwsStorageConfigInfo.Builder storage =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/r")
            .setAllowedLocations(List.of("s3://bucket/base/" + name + "/"));
    if (explicitSts) {
      storage.setCredentialVendingMechanism("STS");
    }
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(name)
        .setProperties(new CatalogProperties("s3://bucket/base/" + name))
        .setStorageConfigInfo(storage.build())
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
}
