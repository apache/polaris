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
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.apache.spark.deploy.SparkSubmitUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Nil$;

/** Shared bundle-jar regression exercise for all Spark versions. */
public abstract class BundleJarSanityITBase {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected abstract String sparkMajorVersion();

  @Test
  void testBundleJarLoading(
      @TempDir Path tempDir, PolarisApiEndpoints endpoints, ClientCredentials credentials)
      throws Exception {
    String bundleJar = System.getProperty("polaris.spark.bundle.jar");
    assertThat(bundleJar).as("polaris.spark.bundle.jar system property").isNotBlank();
    runSparkSqlExercise(
        tempDir, endpoints, credentials, new URL[] {Paths.get(bundleJar).toUri().toURL()});
  }

  @Test
  void testPackagesResolution(
      @TempDir Path tempDir, PolarisApiEndpoints endpoints, ClientCredentials credentials)
      throws Exception {
    String version = System.getProperty("polaris.version");
    assertThat(version).as("polaris.version system property").isNotBlank();
    String scalaVersion = System.getProperty("polaris.scala.version");
    assertThat(scalaVersion).as("polaris.scala.version system property").isNotBlank();
    String coordinate =
        String.format(
            "org.apache.polaris:polaris-spark-%s_%s:%s",
            sparkMajorVersion(), scalaVersion, version);
    URL[] urls = resolveMavenCoordinate(coordinate);
    runSparkSqlExercise(tempDir, endpoints, credentials, urls);
  }

  protected void runSparkSqlExercise(
      Path tempDir, PolarisApiEndpoints endpoints, ClientCredentials credentials, URL[] urls)
      throws Exception {
    String catalogName = "bundle_test_catalog_" + UUID.randomUUID().toString().replace("-", "");
    String token = obtainToken(endpoints, credentials);
    String catalogBaseLocation = tempDir.resolve("catalog").toUri().toString();
    createFileCatalog(endpoints, token, catalogName, catalogBaseLocation);
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    URLClassLoader loader = new URLClassLoader(urls, previous);
    Thread.currentThread().setContextClassLoader(loader);
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
      assertNamespaceAndTableExercise(spark);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
      loader.close();
      deleteCatalog(endpoints, token, catalogName);
    }
  }

  protected void assertNamespaceAndTableExercise(SparkSession spark) {
    spark.sql("USE polaris");
    assertThat(spark.sql("SHOW NAMESPACES").collectAsList()).isEmpty();

    spark.sql("CREATE NAMESPACE bundle_ns");
    assertThat(spark.sql("SHOW NAMESPACES").collectAsList())
        .extracting(row -> row.getString(0))
        .containsExactly("bundle_ns");

    spark.sql("CREATE TABLE bundle_ns.t (id INT, name STRING) USING ICEBERG");
    assertThat(spark.sql("SHOW TABLES IN bundle_ns").collectAsList())
        .extracting(row -> row.getString(1))
        .containsExactly("t");

    spark.sql("INSERT INTO bundle_ns.t VALUES (1, 'a'), (2, 'b')");
    List<Row> rows = spark.sql("SELECT id, name FROM bundle_ns.t ORDER BY id").collectAsList();
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("a");
    assertThat(rows.get(1).getInt(0)).isEqualTo(2);
    assertThat(rows.get(1).getString(1)).isEqualTo("b");

    spark.sql("DROP TABLE bundle_ns.t");
    spark.sql("DROP NAMESPACE bundle_ns");
    assertThat(spark.sql("SHOW NAMESPACES").collectAsList()).isEmpty();
  }

  /**
   * Resolves a Maven coordinate to local jar paths via Spark's own Ivy machinery — the same code
   * path that backs {@code spark-submit --packages}. With {@code useLocalM2=true}, artifacts
   * published by {@code publishToMavenLocal} are picked up from {@code ~/.m2/repository};
   * transitives not in the local repo (e.g. iceberg-spark-runtime) fall through to Maven Central.
   */
  @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
  protected URL[] resolveMavenCoordinate(String coordinate) throws Exception {
    scala.collection.immutable.Seq emptyExclusions =
        (scala.collection.immutable.Seq) Nil$.MODULE$;
    Seq resolved =
        SparkSubmitUtils.resolveMavenCoordinates(
            coordinate,
            SparkSubmitUtils.buildIvySettings(Option.empty(), Option.empty(), true),
            Option.empty(),
            true,
            emptyExclusions,
            false);
    List<URL> urls = new ArrayList<>();
    for (String path : JavaConverters.seqAsJavaList(resolved)) {
      urls.add(new File(path).toURI().toURL());
    }
    assertThat(urls).as("resolved jars for %s", coordinate).isNotEmpty();
    return urls.toArray(new URL[0]);
  }

  protected String obtainToken(PolarisApiEndpoints endpoints, ClientCredentials credentials)
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

  protected void createFileCatalog(
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

  protected void deleteCatalog(PolarisApiEndpoints endpoints, String token, String name)
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
