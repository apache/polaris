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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.R2StorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Runs the shared REST catalog suite against a real Cloudflare R2 bucket. Enabled by the cloudTest
 * subclass only when the INTEGRATION_TEST_R2_* environment is present.
 *
 * <p>The server under test needs polaris.storage.r2.access-key / secret-key. Supply the parent
 * token through the environment (POLARIS_STORAGE_R2_ACCESS_KEY, POLARIS_STORAGE_R2_SECRET_KEY),
 * never through a Quarkus config override: the integration-test launcher turns overrides into
 * command-line arguments and prints them.
 */
public abstract class PolarisRestCatalogR2IntegrationTestBase
    extends PolarisRestCatalogIntegrationBase {

  public static final String BASE_LOCATION = System.getenv("INTEGRATION_TEST_R2_PATH");
  public static final String ACCOUNT_ID = System.getenv("INTEGRATION_TEST_R2_ACCOUNT_ID");
  public static final String JURISDICTION = System.getenv("INTEGRATION_TEST_R2_JURISDICTION");

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    R2StorageConfigInfo.Builder builder =
        R2StorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.R2)
            .setAccountId(ACCOUNT_ID)
            .setAllowedLocations(List.of(BASE_LOCATION));
    if (JURISDICTION != null && !JURISDICTION.isEmpty()) {
      builder.setJurisdiction(JURISDICTION);
    }
    return builder.build();
  }

  private static String endpoint() {
    String host =
        JURISDICTION == null || JURISDICTION.isEmpty()
            ? ACCOUNT_ID + ".r2.cloudflarestorage.com"
            : ACCOUNT_ID + "." + JURISDICTION + ".r2.cloudflarestorage.com";
    return "https://" + host;
  }

  /** Tests that write to storage directly need the R2 endpoint and the parent token. */
  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    return super.clientFileIOProperties()
        .put("s3.endpoint", endpoint())
        .put("s3.path-style-access", "true")
        .put("client.region", "auto")
        .put("s3.access-key-id", requiredEnv("POLARIS_STORAGE_R2_ACCESS_KEY"))
        .put("s3.secret-access-key", requiredEnv("POLARIS_STORAGE_R2_SECRET_KEY"));
  }

  /**
   * Reads an environment variable the R2 suite cannot run without. Fails with the variable name
   * rather than letting a null reach the caller.
   */
  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException(
          "Environment variable " + name + " must be set to run the R2 integration tests");
    }
    return value;
  }

  private static S3Client clientFor(Map<String, String> vended) {
    return S3Client.builder()
        .endpointOverride(java.net.URI.create(vended.get("s3.endpoint")))
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

  /**
   * The June proof, as a permanent assertion: a table's credential cannot write under another
   * table.
   */
  @Test
  public void vendedCredentialIsScopedToItsTablePrefix() {
    Namespace ns = Namespace.of("r2scope");
    catalog().createNamespace(ns);
    TableIdentifier a = TableIdentifier.of(ns, "table_a");
    TableIdentifier b = TableIdentifier.of(ns, "table_b");
    catalog().createTable(a, SCHEMA);
    catalog().createTable(b, SCHEMA);

    LoadTableResponse loadedA =
        catalogApi().loadTableWithAccessDelegation(currentCatalogName(), a, "all");
    LoadTableResponse loadedB =
        catalogApi().loadTableWithAccessDelegation(currentCatalogName(), b, "all");
    assertThat(loadedA.config()).containsKey("s3.session-token");

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
}
