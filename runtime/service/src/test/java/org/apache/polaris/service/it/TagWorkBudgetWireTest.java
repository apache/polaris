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

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.TagApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.ListObjectsByTagResponse;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TargetType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins, on the wire, what a reverse lookup answers when its value filter has to examine more
 * candidates than the request's work budget allows. The in-memory store charges every row it
 * examines, so this is the store where a value nobody uses on a small page reaches the budget
 * before the range ends; a relational store bounds the rows it returns per call and its scan is not
 * measured, so the same request there answers the empty page and this class does not run against
 * it. The three answers pinned here are the refusal, the ordinary empty page when the budget covers
 * the range, and the ordinary page when the value is carried.
 */
@QuarkusTest
@TestProfile(Profiles.TagStoreProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class TagWorkBudgetWireTest {

  /** More rows than a page of one may examine (twenty), fewer than a page of two may (forty). */
  private static final int ROWS = 25;

  private static final String TAG = "budgeted";

  private PolarisClient client;
  private ManagementApi managementApi;
  private TagApi tagApi;
  private URI baseLocation;
  private String currentCatalogName;

  @BeforeAll
  void setup(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    client = polarisClient(apiEndpoints);
    managementApi = client.managementApi(client.obtainToken(credentials));
    baseLocation = IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve("data");
  }

  @AfterAll
  void close() throws Exception {
    client.close();
  }

  @BeforeEach
  void createCatalogWithRowsCarryingOneValue(TestInfo testInfo) {
    String principalName = "tag-budget-" + UUID.randomUUID();
    String principalRoleName = "tag-budget-admin-" + UUID.randomUUID();
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);

    currentCatalogName = client.newEntityName(testInfo.getTestMethod().orElseThrow().getName());
    String catalogLocation = baseLocation.resolve(currentCatalogName).toString();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(currentCatalogName)
            .setProperties(CatalogProperties.builder(catalogLocation).build())
            .setStorageConfigInfo(
                new FileStorageConfigInfo(
                    StorageConfigInfo.StorageTypeEnum.FILE, List.of(catalogLocation), null))
            .build();
    managementApi.createCatalog(principalRoleName, catalog);

    String catalogRoleName = "tag-budget-role";
    managementApi.createCatalogRole(currentCatalogName, catalogRoleName);
    managementApi.addGrant(
        currentCatalogName,
        catalogRoleName,
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG));
    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName,
        currentCatalogName,
        managementApi.getCatalogRole(currentCatalogName, catalogRoleName));

    tagApi = client.tagApi(client.obtainToken(principalCredentials));
    tagApi.createTag(
        currentCatalogName, TAG, "a comment", List.of("a", "b"), List.of(TargetType.NAMESPACE));
    for (int i = 0; i < ROWS; i++) {
      String namespace = "ns" + i;
      try (Response res =
          tagApi
              .request("v1/{cat}/namespaces", Map.of("cat", currentCatalogName))
              .post(Entity.json(Map.of("namespace", List.of(namespace))))) {
        Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      }
      tagApi.assignTag(
          currentCatalogName,
          TAG,
          TagAttachmentTarget.builder(TargetType.NAMESPACE).setPath(List.of(namespace)).build(),
          List.of("a"));
    }
  }

  @Test
  public void testAValueNobodyUsesIsRefusedWhenTheBudgetRunsOutBeforeTheRangeEnds() {
    try (Response res = lookup("b", "1")) {
      String body = res.readEntity(String.class);
      Assertions.assertThat(res.getStatus())
          .as(body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(body).contains("\"type\":\"BadRequest\"").contains("\"code\":400");
      Assertions.assertThat(body)
          .contains(
              "This request examined more candidate assignments than its work budget allows"
                  + " (limit 20) without finding one it could return or resume from; the budget"
                  + " grows with the page size, up to the deployment's maximum page size, and a"
                  + " client that sent a value filter can instead page without it and apply the"
                  + " filter to the pages it receives");
    }
  }

  @Test
  public void testAValueNobodyUsesAnswersTheEmptyPageWhenTheBudgetCoversTheRange() {
    try (Response res = lookup("b", "2")) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      ListObjectsByTagResponse page = res.readEntity(ListObjectsByTagResponse.class);
      Assertions.assertThat(page.getObjects()).isEmpty();
      Assertions.assertThat(page.getNextPageToken()).isNull();
    }
  }

  @Test
  public void testAValueTheRowsCarryAnswersThePage() {
    try (Response res = lookup("a", "1")) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      ListObjectsByTagResponse page = res.readEntity(ListObjectsByTagResponse.class);
      Assertions.assertThat(page.getObjects()).hasSize(1);
      Assertions.assertThat(page.getNextPageToken()).isNotNull().isNotEmpty();
    }
  }

  private Response lookup(String value, String pageSize) {
    return tagApi
        .request(
            "polaris/v1/{cat}/tags/{tag}/assignments",
            Map.of("cat", currentCatalogName, "tag", TAG),
            Map.of("value", value, "pageSize", pageSize))
        .get();
  }
}
