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
package org.apache.polaris.spark.quarkus.it;

import static jakarta.ws.rs.core.Response.Status.CREATED;
import static jakarta.ws.rs.core.Response.Status.NO_CONTENT;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** Sanity test exercising the polaris-spark shaded bundle jar on a curated classpath. */
@QuarkusIntegrationTest
@ExtendWith(PolarisIntegrationTestExtension.class)
public class BundleJarSanityIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void testBundleJarLoading(
      @TempDir Path tempDir, PolarisApiEndpoints endpoints, ClientCredentials credentials)
      throws Exception {
    String catalogName = "bundle_test_catalog_" + UUID.randomUUID().toString().replace("-", "");
    String token = obtainToken(endpoints, credentials);
    String catalogBaseLocation = tempDir.resolve("catalog").toUri().toString();
    createFileCatalog(endpoints, token, catalogName, catalogBaseLocation);
    try (SparkSession spark =
        SparkSession.builder()
            .master("local")
            .config("spark.sql.catalog.polaris", "org.apache.polaris.spark.SparkCatalog")
            .config("spark.sql.catalog.polaris.type", "rest")
            .config("spark.sql.catalog.polaris.uri", endpoints.catalogApiEndpoint().toString())
            .config("spark.sql.catalog.polaris.warehouse", catalogName)
            .config("spark.sql.catalog.polaris.token", token)
            .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toString())
            .getOrCreate()) {
      spark.sql("USE polaris");
      spark.sql("CREATE NAMESPACE bundle_ns");
      spark.sql("CREATE TABLE bundle_ns.t (id INT, name STRING) USING ICEBERG");
      spark.sql("INSERT INTO bundle_ns.t VALUES (1, 'a'), (2, 'b')");
      assertThat(spark.sql("SELECT * FROM bundle_ns.t").count()).isEqualTo(2);
      spark.sql("DROP TABLE bundle_ns.t");
      spark.sql("DROP NAMESPACE bundle_ns");
    } finally {
      deleteCatalog(endpoints, token, catalogName);
    }
  }

  private static String obtainToken(PolarisApiEndpoints endpoints, ClientCredentials credentials)
      throws Exception {
    try (RESTClient restClient =
        HTTPClient.builder(Map.of()).uri(endpoints.catalogApiEndpoint()).build()) {
      OAuthTokenResponse response =
          OAuth2Util.fetchToken(
              restClient.withAuthSession(AuthSession.EMPTY),
              Map.of(),
              String.format("%s:%s", credentials.clientId(), credentials.clientSecret()),
              "PRINCIPAL_ROLE:ALL",
              endpoints.catalogApiEndpoint() + "/v1/oauth/tokens",
              Map.of("grant_type", "client_credentials"));
      return response.token();
    }
  }

  private static void createFileCatalog(
      PolarisApiEndpoints endpoints, String token, String name, String baseLocation)
      throws Exception {
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(name)
            .setProperties(new CatalogProperties(baseLocation))
            .setStorageConfigInfo(
                FileStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                    .setAllowedLocations(List.of(baseLocation))
                    .build())
            .build();
    URI uri = URI.create(endpoints.managementApiEndpoint() + "/v1/catalogs");
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer " + token)
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(catalog)));
    endpoints.extraHeaders().forEach(builder::header);
    HttpResponse<String> response =
        HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode())
        .withFailMessage("create catalog failed: %d %s", response.statusCode(), response.body())
        .isEqualTo(CREATED.getStatusCode());
  }

  private static void deleteCatalog(PolarisApiEndpoints endpoints, String token, String name)
      throws Exception {
    URI uri = URI.create(endpoints.managementApiEndpoint() + "/v1/catalogs/" + name);
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(uri).header("Authorization", "Bearer " + token).DELETE();
    endpoints.extraHeaders().forEach(builder::header);
    HttpResponse<byte[]> response =
        HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    assertThat(response.statusCode())
        .withFailMessage("delete catalog failed: %d", response.statusCode())
        .isEqualTo(NO_CONTENT.getStatusCode());
  }
}
