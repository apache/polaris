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

import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.DirectoryApi;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of the directories REST API on a file based catalog: the whole create, scan, load
 * and drop flow goes through the REST layer, including authorization and location validation.
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisDirectoryServiceIntegrationTest {

  private static final String CATALOG_ROLE = "directory_catalog_role";
  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier IMAGES = TableIdentifier.of(NS, "images");
  private static final TableIdentifier IMAGES_INVENTORY =
      TableIdentifier.of(NS, "images__inventory");

  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static String adminToken;
  private static URI rootUri;

  private RESTCatalog restCatalog;
  private DirectoryApi directoryApi;
  private String catalogName;
  private URI catalogBaseLocation;

  @BeforeAll
  public static void setup(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    rootUri = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
  }

  @AfterAll
  public static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before() {
    String principalRoleName = "directory-admin-" + UUID.randomUUID();
    PrincipalWithCredentials principal =
        managementApi.createPrincipalWithRole(
            "directory-user-" + UUID.randomUUID(), principalRoleName);

    catalogName = client.newEntityName("directory");
    catalogBaseLocation = rootUri.resolve(catalogName + "/");
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(
                CatalogProperties.builder(catalogBaseLocation.toString())
                    // directories are rooted outside the namespace location
                    .putAll(Map.of("polaris.config.allow.unstructured.table.location", "true"))
                    .build())
            .setStorageConfigInfo(
                new FileStorageConfigInfo(
                    StorageConfigInfo.StorageTypeEnum.FILE,
                    List.of(catalogBaseLocation.toString()),
                    null))
            .build();
    managementApi.createCatalog(principalRoleName, catalog);

    managementApi.createCatalogRole(catalogName, CATALOG_ROLE);
    managementApi.addGrant(
        catalogName,
        CATALOG_ROLE,
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG));
    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName, catalogName, managementApi.getCatalogRole(catalogName, CATALOG_ROLE));

    String principalToken = client.obtainToken(principal);
    restCatalog = IcebergHelper.restCatalog(endpoints, catalogName, Map.of(), principalToken);
    directoryApi = client.directoryApi(principalToken);
    restCatalog.createNamespace(NS);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } finally {
      client.cleanUp(adminToken);
    }
  }

  @Test
  public void testCreateScanLoadAndDrop() throws IOException {
    URI baseLocation = catalogBaseLocation.resolve("images/");
    Path baseDir = Path.of(baseLocation);
    Files.createDirectories(baseDir);
    Files.writeString(baseDir.resolve("a.jpg"), "a");
    Files.writeString(baseDir.resolve("b.png"), "bb");

    directoryApi.createDirectory(catalogName, IMAGES, baseLocation.toString(), Map.of());

    assertThat(directoryApi.listDirectories(catalogName, NS)).containsExactly(IMAGES);
    assertThat(directoryApi.getDirectory(catalogName, IMAGES).getBaseLocation())
        .isEqualTo(baseLocation.toString());
    // The inventory table cannot share the name of the directory
    assertThat(restCatalog.tableExists(IMAGES_INVENTORY)).isTrue();

    assertThat(directoryApi.scanDirectory(catalogName, IMAGES)).isEqualTo(2L);
    assertThat(restCatalog.loadTable(IMAGES_INVENTORY).currentSnapshot()).isNotNull();

    directoryApi.dropDirectory(catalogName, IMAGES);
    assertThat(directoryApi.listDirectories(catalogName, NS)).isEmpty();
    assertThat(restCatalog.tableExists(IMAGES_INVENTORY)).isFalse();
  }

  @Test
  public void testCreateOutsideAllowedLocationsIsForbidden() {
    URI outside = rootUri.resolve("not-allowed-" + UUID.randomUUID() + "/");

    try (Response response =
        directoryApi.tryCreateDirectory(catalogName, IMAGES, outside.toString())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.FORBIDDEN.getStatusCode());
    }
    assertThat(directoryApi.listDirectories(catalogName, NS)).isEmpty();
    assertThat(restCatalog.tableExists(IMAGES_INVENTORY)).isFalse();
  }
}
