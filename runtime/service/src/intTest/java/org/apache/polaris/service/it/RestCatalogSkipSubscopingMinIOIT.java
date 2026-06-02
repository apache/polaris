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

import static org.apache.iceberg.CatalogProperties.TABLE_DEFAULT_PREFIX;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_KEY_ID;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_SECRET_KEY;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.test.minio.Minio;
import org.apache.polaris.test.minio.MinioAccess;
import org.apache.polaris.test.minio.MinioExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Integration tests for catalogs backed by a custom S3-compatible endpoint when
 * {@code SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION} is enabled.
 *
 * <p>Regression test for: when subscoping is skipped, {@link
 * org.apache.polaris.service.catalog.io.StorageAccessConfigProvider} previously returned an empty
 * {@link org.apache.polaris.core.storage.StorageAccessConfig}, losing the S3 endpoint and
 * path-style-access settings. This caused Iceberg's {@code S3FileIO} to fall back to
 * virtual-hosted-style addressing, resulting in TLS hostname-mismatch errors when the storage
 * endpoint certificate does not carry a wildcard SAN.
 */
@QuarkusIntegrationTest
@TestProfile(RestCatalogSkipSubscopingMinIOIT.Profile.class)
@ExtendWith(MinioExtension.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class RestCatalogSkipSubscopingMinIOIT {

  private static final String BUCKET_URI_PREFIX = "/minio-skip-subscoping-test";
  private static final String MINIO_ACCESS_KEY = "test-ak-skip";
  private static final String MINIO_SECRET_KEY = "test-sk-skip";

  private static String adminToken;

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.storage.aws.access-key", MINIO_ACCESS_KEY)
          .put("polaris.storage.aws.secret-key", MINIO_SECRET_KEY)
          // Enable the skip-subscoping mode that triggered the bug
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "true")
          .build();
    }
  }

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static URI storageBase;
  private static String s3Endpoint;
  private static S3Client s3Client;

  private CatalogApi catalogApi;
  private String principalRoleName;
  private PrincipalWithCredentials principalCredentials;
  private String catalogName;

  @BeforeAll
  static void setup(
      PolarisApiEndpoints apiEndpoints,
      @Minio(accessKey = MINIO_ACCESS_KEY, secretKey = MINIO_SECRET_KEY) MinioAccess minioAccess,
      ClientCredentials credentials) {
    s3Client = minioAccess.s3Client();
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    storageBase = minioAccess.s3BucketUri(BUCKET_URI_PREFIX);
    s3Endpoint = minioAccess.s3endpoint();
  }

  @AfterAll
  static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    String principalName = client.newEntityName("test-user");
    principalRoleName = client.newEntityName("test-admin");
    principalCredentials = managementApi.createPrincipalWithRole(principalName, principalRoleName);
    String principalToken = client.obtainToken(principalCredentials);
    catalogApi = client.catalogApi(principalToken);
    catalogName =
        client.newEntityName(testInfo.getTestMethod().orElseThrow().getName());
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(adminToken);
  }

  /**
   * Verifies that Polaris can write table metadata to a path-style S3-compatible endpoint when
   * credential subscoping is skipped. Before the fix, this would fail with a TLS PKIX error caused
   * by S3FileIO defaulting to virtual-hosted-style addressing (e.g.
   * {@code bucket.minio-host/key}) whose hostname is not covered by the server's certificate.
   */
  @Test
  public void testCreateTableWithPathStyleEndpointAndSkipSubscoping() {
    AwsStorageConfigInfo storageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setEndpoint(s3Endpoint)
            .setPathStyleAccess(true)
            .setStsUnavailable(true)
            .setAllowedLocations(List.of(storageBase.toString()))
            .build();

    CatalogProperties catalogProps =
        CatalogProperties.builder(storageBase.toASCIIString() + "/" + catalogName)
            // Provide static credentials via table-default so S3FileIO can authenticate;
            // with subscoping skipped these are not vended dynamically.
            .addProperty(TABLE_DEFAULT_PREFIX + AWS_KEY_ID.getPropertyName(), MINIO_ACCESS_KEY)
            .addProperty(TABLE_DEFAULT_PREFIX + AWS_SECRET_KEY.getPropertyName(), MINIO_SECRET_KEY)
            .build();

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(storageConfig)
            .setProperties(catalogProps)
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    try (RESTCatalog restCatalog = new RESTCatalog()) {
      restCatalog.initialize(
          "polaris",
          ImmutableMap.<String, String>builder()
              .put(
                  org.apache.iceberg.CatalogProperties.URI,
                  endpoints.catalogApiEndpoint().toString())
              .put(OAuth2Properties.TOKEN, client.obtainToken(principalCredentials))
              .put("warehouse", catalogName)
              .putAll(endpoints.extraHeaders("header."))
              // Client-side credentials for the RESTCatalog to write data files
              .put("s3.access-key-id", MINIO_ACCESS_KEY)
              .put("s3.secret-access-key", MINIO_SECRET_KEY)
              .put("s3.endpoint", s3Endpoint)
              .put("s3.path-style-access", "true")
              .buildKeepingLast());

      catalogApi.createNamespace(catalogName, "test-ns");
      TableIdentifier id = TableIdentifier.of("test-ns", "t1");

      // This createTable triggers Polaris to write metadata.json to S3 via S3FileIO.
      // Before the fix, S3FileIO received no endpoint/path-style config and fell back
      // to virtual-hosted addressing, causing a TLS hostname-mismatch failure.
      Table table = restCatalog.createTable(id, SCHEMA);
      assertThat(table).isNotNull();

      // Verify the metadata file was actually written to the path-style S3 endpoint
      TableOperations ops = ((HasTableOperations) table).operations();
      URI metadataLocation = URI.create(ops.current().metadataFileLocation());
      GetObjectResponse response =
          s3Client
              .getObject(
                  GetObjectRequest.builder()
                      .bucket(metadataLocation.getAuthority())
                      .key(metadataLocation.getPath().substring(1))
                      .build())
              .response();
      assertThat(response.contentLength()).isGreaterThan(0);

      restCatalog.dropTable(id);
    }
  }

  /**
   * Verifies that the S3 endpoint and path-style config are exposed in the loadTable response
   * even when subscoping is skipped, so clients can configure their FileIO correctly.
   */
  @Test
  public void testLoadTableResponseContainsEndpointAndPathStyleWithSkipSubscoping() {
    AwsStorageConfigInfo storageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setEndpoint(s3Endpoint)
            .setPathStyleAccess(true)
            .setStsUnavailable(true)
            .setAllowedLocations(List.of(storageBase.toString()))
            .build();

    CatalogProperties catalogProps =
        CatalogProperties.builder(storageBase.toASCIIString() + "/" + catalogName)
            .addProperty(TABLE_DEFAULT_PREFIX + AWS_KEY_ID.getPropertyName(), MINIO_ACCESS_KEY)
            .addProperty(TABLE_DEFAULT_PREFIX + AWS_SECRET_KEY.getPropertyName(), MINIO_SECRET_KEY)
            .build();

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(storageConfig)
            .setProperties(catalogProps)
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    try (RESTCatalog restCatalog = new RESTCatalog()) {
      restCatalog.initialize(
          "polaris",
          ImmutableMap.<String, String>builder()
              .put(
                  org.apache.iceberg.CatalogProperties.URI,
                  endpoints.catalogApiEndpoint().toString())
              .put(OAuth2Properties.TOKEN, client.obtainToken(principalCredentials))
              .put("warehouse", catalogName)
              .putAll(endpoints.extraHeaders("header."))
              .put("s3.access-key-id", MINIO_ACCESS_KEY)
              .put("s3.secret-access-key", MINIO_SECRET_KEY)
              .put("s3.endpoint", s3Endpoint)
              .put("s3.path-style-access", "true")
              .buildKeepingLast());

      catalogApi.createNamespace(catalogName, "test-ns");
      TableIdentifier id = TableIdentifier.of("test-ns", "t1");
      restCatalog.createTable(id, SCHEMA);

      var loadTableResponse = catalogApi.loadTable(catalogName, id, "ALL", Map.of());
      assertThat(loadTableResponse.config())
          .containsEntry("s3.endpoint", s3Endpoint)
          .containsEntry("s3.path-style-access", "true");

      restCatalog.dropTable(id);
    }
  }
}
