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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Checks that the packaged Polaris server includes the {@code R2_LOCAL_SIGNING} credential vending
 * mechanism.
 *
 * <p>The server is the {@code :polaris-server} runner jar. The {@code polaris-server-test-runner}
 * plugin starts it for this suite, with the realm allowlist and a dummy parent token set in {@code
 * build.gradle.kts}. The test makes no call to Cloudflare R2, so it runs in upstream CI.
 *
 * <p>A server without the bean answers a catalog create with HTTP 400 ("S3 credential vending
 * mechanism R2_LOCAL_SIGNING is not available in this server"). So a 201 shows that the Jandex
 * index and the {@code runtimeOnly} dependency work. The suite also shows that the packaged server
 * starts with the extension, and that the configuration mapping of the extension ({@code
 * polaris.storage.cloudflare-r2}) loads together with the core configuration.
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class R2LocalSigningDiscoveryIT {

  private static final String ENDPOINT =
      "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com";
  private static final String BASE_LOCATION = "s3://test-bucket/r2/";

  private static PolarisClient client;
  private static ManagementApi managementApi;

  @BeforeAll
  public static void setup(PolarisApiEndpoints endpoints, ClientCredentials credentials) {
    client = polarisClient(endpoints);
    managementApi = client.managementApi(client.obtainToken(credentials));
  }

  @AfterAll
  public static void close() throws Exception {
    client.close();
  }

  @Test
  public void allowlistedR2LocalSigningCatalogIsCreatedAndRead() {
    String name = client.newEntityName("r2_local_signing");

    try (Response response = create(name, "R2_LOCAL_SIGNING")) {
      String body = response.readEntity(String.class);
      assertThat(response.getStatus())
          .as("create response body: %s", body)
          .isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    Catalog stored = managementApi.getCatalog(name);
    assertThat(stored.getStorageConfigInfo()).isInstanceOf(AwsStorageConfigInfo.class);
    AwsStorageConfigInfo storage = (AwsStorageConfigInfo) stored.getStorageConfigInfo();
    assertThat(storage.getCredentialVendingMechanism()).isEqualTo("R2_LOCAL_SIGNING");
    assertThat(storage.getEndpoint()).isEqualTo(ENDPOINT);
    assertThat(storage.getPathStyleAccess()).isTrue();
    assertThat(storage.getRegion()).isEqualTo("auto");
  }

  @Test
  public void mechanismOutsideTheRealmAllowlistIsRefused() {
    String name = client.newEntityName("r2_other_mechanism");

    try (Response response = create(name, "OTHER_MECHANISM")) {
      String body = response.readEntity(String.class);
      assertThat(response.getStatus())
          .as("create response body: %s", body)
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      assertThat(body)
          .contains("S3 credential vending mechanism OTHER_MECHANISM is not enabled in this realm");
    }
  }

  private static Response create(String name, String mechanism) {
    return managementApi.request("v1/catalogs").post(Entity.json(r2Catalog(name, mechanism)));
  }

  private static Catalog r2Catalog(String name, String mechanism) {
    AwsStorageConfigInfo storage =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism(mechanism)
            .setEndpoint(ENDPOINT)
            .setPathStyleAccess(true)
            .setRegion("auto")
            .setAllowedLocations(List.of(BASE_LOCATION))
            .build();
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName(name)
        .setProperties(new CatalogProperties(BASE_LOCATION))
        .setStorageConfigInfo(storage)
        .build();
  }
}
