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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.ACCESS_KEY_ENV;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.BASE_LOCATION;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.SECRET_KEY_ENV;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.accountId;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.cloudflareR2StorageConfig;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.endpoint;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.requiredEnv;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Live tests against a real Cloudflare R2 bucket through an S3 catalog whose {@code
 * credentialVendingMechanism} is {@code R2_LOCAL_SIGNING}. They check what the unit tests cannot:
 * what R2 does with a vended credential. The class runs only when the {@code INTEGRATION_TEST_R2_*}
 * environment is present, as {@link RestCatalogR2IT} does.
 *
 * <p>Each test creates its own principal, catalog, and namespace through {@link PolarisClient} and
 * removes them after the run. The server under test needs {@code
 * polaris.storage.cloudflare-r2.access-key} and {@code secret-key}. Supply the parent token through
 * the environment ({@code POLARIS_STORAGE_CLOUDFLARE_R2_ACCESS_KEY} and {@code
 * POLARIS_STORAGE_CLOUDFLARE_R2_SECRET_KEY}), never through a system property or a Gradle task
 * input: the polaris-server-test-runner turns system properties into command-line arguments of the
 * server process, and Gradle records task inputs. The server and this test JVM both inherit the
 * environment of the Gradle process.
 *
 * <p>Tests never print the parent token, never put it in an assertion message, and never log client
 * properties.
 */
@EnabledIfEnvironmentVariable(named = "INTEGRATION_TEST_R2_PATH", matches = ".+")
@ExtendWith(PolarisIntegrationTestExtension.class)
public class R2LocalSigningLiveIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(R2LocalSigningLiveIT.class);

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  /** Catalog properties of the shared REST catalog suite, which the first test relies on. */
  private static final Map<String, String> CATALOG_PROPERTIES =
      Map.of(
          "polaris.config.allow.unstructured.table.location",
          "true",
          "polaris.config.allow.external.metadata.file.location",
          "true",
          "polaris.config.list-pagination-enabled",
          "true",
          "polaris.config.namespace-custom-location.enabled",
          "true",
          FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(),
          "true");

  private static PolarisApiEndpoints endpoints;
  private static ClientCredentials adminCredentials;
  private static PolarisClient client;

  private String adminToken;
  private ManagementApi managementApi;
  private CatalogApi catalogApi;
  private String principalRoleName;
  private String catalogName;
  private RESTCatalog restCatalog;

  @BeforeAll
  static void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    endpoints = apiEndpoints;
    adminCredentials = credentials;
    client = polarisClient(endpoints);
  }

  @AfterAll
  static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  void before(TestInfo testInfo) {
    adminToken = client.obtainToken(adminCredentials);
    managementApi = client.managementApi(adminToken);

    principalRoleName = client.newEntityName("r2-live-admin");
    PrincipalWithCredentials principal =
        managementApi.createPrincipalWithRole(client.newEntityName("r2-live"), principalRoleName);
    String principalToken = client.obtainToken(principal);
    catalogApi = client.catalogApi(principalToken);

    catalogName = client.newEntityName(testInfo.getTestMethod().orElseThrow().getName());
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(
                CatalogProperties.builder(BASE_LOCATION + "/live/" + catalogName)
                    .putAll(CATALOG_PROPERTIES)
                    .build())
            .setStorageConfigInfo(
                cloudflareR2StorageConfig(endpoint(accountId()), List.of(BASE_LOCATION)))
            .build();
    managementApi.createCatalog(principalRoleName, catalog);

    restCatalog =
        IcebergHelper.restCatalog(
            endpoints,
            catalogName,
            ImmutableMap.of(
                "header.X-Iceberg-Access-Delegation",
                "vended-credentials",
                org.apache.iceberg.CatalogProperties.FILE_IO_IMPL,
                ResolvingFileIO.class.getName()),
            principalToken);
  }

  @AfterEach
  void cleanUp() throws IOException {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } finally {
      client.cleanUp(adminToken);
    }
  }

  private static S3Client clientFor(Map<String, String> vended) {
    return S3Client.builder()
        .endpointOverride(URI.create(vended.get("s3.endpoint")))
        .region(Region.of("auto"))
        .forcePathStyle(true)
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    vended.get("s3.access-key-id"),
                    vended.get("s3.secret-access-key"),
                    vended.get("s3.session-token"))))
        .build();
  }

  /** A client authenticated with the parent token itself, for test fixtures Polaris must find. */
  private static S3Client parentTokenClient() {
    return S3Client.builder()
        .endpointOverride(URI.create(endpoint(accountId())))
        .region(Region.of("auto"))
        .forcePathStyle(true)
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    requiredEnv(ACCESS_KEY_ENV), requiredEnv(SECRET_KEY_ENV))))
        .build();
  }

  /**
   * Splits an s3:// location into bucket and key prefix. A location with no path has an empty key.
   */
  private static String[] bucketAndKey(String location, String suffix) {
    String noScheme = location.substring(location.indexOf("://") + 3);
    int slash = noScheme.indexOf('/');
    String bucket = slash < 0 ? noScheme : noScheme.substring(0, slash);
    String key = slash < 0 ? "" : noScheme.substring(slash + 1);
    if (!key.isEmpty() && !key.endsWith("/")) {
      key = key + "/";
    }
    return new String[] {bucket, key + suffix};
  }

  /** A table's vended credential must not be able to write under a sibling table's prefix. */
  @Test
  public void vendedCredentialIsScopedToItsTablePrefix() {
    Namespace ns = Namespace.of("r2scope");
    restCatalog.createNamespace(ns);
    TableIdentifier a = TableIdentifier.of(ns, "table_a");
    TableIdentifier b = TableIdentifier.of(ns, "table_b");
    restCatalog.createTable(a, SCHEMA);
    restCatalog.createTable(b, SCHEMA);

    LoadTableResponse loadedA = catalogApi.loadTableWithAccessDelegation(catalogName, a, "all");
    LoadTableResponse loadedB = catalogApi.loadTableWithAccessDelegation(catalogName, b, "all");
    // Assert on a boolean, so a failure never prints the vended credentials in the map.
    assertThat(loadedA.config().containsKey("s3.session-token"))
        .as("the vended configuration carries a session token")
        .isTrue();

    try (S3Client asA = clientFor(loadedA.config())) {
      String[] ownTarget = bucketAndKey(loadedA.tableMetadata().location(), "probe.txt");
      asA.putObject(
          PutObjectRequest.builder().bucket(ownTarget[0]).key(ownTarget[1]).build(),
          RequestBody.fromString("ok"));

      try {
        String[] otherTarget = bucketAndKey(loadedB.tableMetadata().location(), "probe.txt");
        assertThatThrownBy(
                () ->
                    asA.putObject(
                        PutObjectRequest.builder()
                            .bucket(otherTarget[0])
                            .key(otherTarget[1])
                            .build(),
                        RequestBody.fromString("nope")))
            .isInstanceOf(S3Exception.class)
            .satisfies(e -> assertThat(((S3Exception) e).statusCode()).isEqualTo(403));
      } finally {
        // Leave the bucket as we found it, whether or not the assertion above held.
        asA.deleteObject(
            DeleteObjectRequest.builder().bucket(ownTarget[0]).key(ownTarget[1]).build());
      }
    }
  }

  /**
   * A catalog whose endpoint names a well-formed account id that is not the parent token's account
   * passes creation (nothing touches R2 at create) and cannot complete a registration: the server's
   * own first vended access, its read of the metadata file during {@code registerTable}, is signed
   * for the foreign account and addressed to that account's host. The metadata file is written
   * first with the parent token, so a missing object cannot be the cause and a 404 is rejected; an
   * implementation that signed for and addressed the real account by mistake would make the
   * register call succeed and this test fail. Against real R2, Cloudflare serves no TLS for an
   * unknown account host, so the S3 client fails in the TLS handshake and R2 never evaluates the
   * token. The case therefore proves that a wrong endpoint reaches no data. It does not prove that
   * R2 rejects a token signed for a foreign account; that needs a second real account. The failure
   * text is logged as evidence and no error code is pinned.
   */
  @Test
  public void foreignEndpointPassesCreateAndCannotCompleteRegistration() {
    String foreignCatalogName = catalogName + "_foreign";
    String foreignBase = BASE_LOCATION + "/foreign-account/" + catalogName;
    Catalog foreign =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(foreignCatalogName)
            .setProperties(new CatalogProperties(foreignBase))
            .setStorageConfigInfo(
                cloudflareR2StorageConfig(
                    endpoint(foreignAccountId(accountId())), List.of(foreignBase)))
            .build();
    managementApi.createCatalog(principalRoleName, foreign);
    assertThat(managementApi.getCatalog(foreignCatalogName).getStorageConfigInfo())
        .isInstanceOf(AwsStorageConfigInfo.class);
    catalogApi.createNamespace(foreignCatalogName, "ns");

    // A real, readable metadata file in the real account, at the location the register names.
    String tableLocation = foreignBase + "/ns/t";
    String metadataLocation = tableLocation + "/metadata/00000-foreign.metadata.json";
    String[] metadataObject =
        bucketAndKey(tableLocation + "/metadata", "00000-foreign.metadata.json");
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), tableLocation, Map.of());
    try (S3Client asParent = parentTokenClient()) {
      asParent.putObject(
          PutObjectRequest.builder().bucket(metadataObject[0]).key(metadataObject[1]).build(),
          RequestBody.fromString(TableMetadataParser.toJson(metadata)));
      try {
        Throwable failure =
            catchThrowable(
                () ->
                    catalogApi.registerTable(
                        foreignCatalogName, Namespace.of("ns"), "t", metadataLocation, false));
        assertThat(failure)
            .as(
                "a credential signed for and addressed to a foreign account must not complete the"
                    + " registration")
            .isNotNull();
        assertThat(failure)
            .as("the object exists, so the failure cannot be a missing file")
            .hasMessageNotContaining("404")
            .hasMessageNotContaining("NoSuchKey");
        LOGGER.info(
            "Registration against a foreign-account endpoint failed at the server's first vended"
                + " access: {}",
            failure.toString());
      } finally {
        // Leave the bucket as we found it, whether or not the assertions above held.
        asParent.deleteObject(
            DeleteObjectRequest.builder().bucket(metadataObject[0]).key(metadataObject[1]).build());
      }
    }
  }

  /**
   * Flips the first hex digit, so the id stays 32 lowercase hex characters but names no account.
   */
  static String foreignAccountId(String accountId) {
    char flipped = accountId.charAt(0) == '0' ? '1' : '0';
    return flipped + accountId.substring(1);
  }
}
