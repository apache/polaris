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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.CatalogProperties.TABLE_DEFAULT_PREFIX;
import static org.apache.iceberg.aws.AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ACCESS_KEY_ID;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ENDPOINT;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.SECRET_ACCESS_KEY;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_KEY_ID;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_SECRET_KEY;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.catalog.AccessDelegationMode;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * These tests complement {@link PolarisRestCatalogMinIOIT} to validate client-side access to MinIO
 * storage via {@code FileIO} instances configured from catalog's {@code loadTable} responses with
 * some S3-specific options.
 */
@QuarkusTest
@TestProfile(RestCatalogMinIOSpecialTest.Profile.class)
@ExtendWith(MinioExtension.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class RestCatalogMinIOSpecialTest {

  private static final String BUCKET_URI_PREFIX = "/minio-test";
  private static final String MINIO_ACCESS_KEY = "test-ak-123";
  private static final String MINIO_SECRET_KEY = "test-sk-123";
  private static String adminToken;

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.storage.aws.access-key", MINIO_ACCESS_KEY)
          .put("polaris.storage.aws.secret-key", MINIO_SECRET_KEY)
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "false")
          .build();
    }
  }

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "doc"),
          optional(2, "data", Types.StringType.get()));

  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static URI storageBase;
  private static String endpoint;
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
    endpoint = minioAccess.s3endpoint();
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

    catalogName = client.newEntityName(testInfo.getTestMethod().orElseThrow().getName());
  }

  private RESTCatalog createCatalog(
      Optional<String> endpoint,
      Optional<String> stsEndpoint,
      boolean pathStyleAccess,
      Optional<AccessDelegationMode> delegationMode,
      boolean stsEnabled) {
    return createCatalog(
        endpoint, stsEndpoint, pathStyleAccess, Optional.empty(), delegationMode, stsEnabled);
  }

  private RESTCatalog createCatalog(
      Optional<String> endpoint,
      Optional<String> stsEndpoint,
      boolean pathStyleAccess,
      Optional<String> endpointInternal,
      Optional<AccessDelegationMode> delegationMode,
      boolean stsEnabled) {
    AwsStorageConfigInfo.Builder storageConfig =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setPathStyleAccess(pathStyleAccess)
            .setStsUnavailable(!stsEnabled)
            .setAllowedLocations(List.of(storageBase.toString()));

    endpoint.ifPresent(storageConfig::setEndpoint);
    stsEndpoint.ifPresent(storageConfig::setStsEndpoint);
    endpointInternal.ifPresent(storageConfig::setEndpointInternal);

    CatalogProperties.Builder catalogProps =
        CatalogProperties.builder(storageBase.toASCIIString() + "/" + catalogName);
    if (!stsEnabled) {
      catalogProps.addProperty(
          TABLE_DEFAULT_PREFIX + AWS_KEY_ID.getPropertyName(), MINIO_ACCESS_KEY);
      catalogProps.addProperty(
          TABLE_DEFAULT_PREFIX + AWS_SECRET_KEY.getPropertyName(), MINIO_SECRET_KEY);
    }
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(storageConfig.build())
            .setProperties(catalogProps.build())
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    String authToken = client.obtainToken(principalCredentials);
    RESTCatalog restCatalog = new RESTCatalog();

    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(
                org.apache.iceberg.CatalogProperties.URI, endpoints.catalogApiEndpoint().toString())
            .put(OAuth2Properties.TOKEN, authToken)
            .put("warehouse", catalogName)
            .putAll(endpoints.extraHeaders("header."));

    delegationMode.ifPresent(
        dm -> propertiesBuilder.put("header.X-Iceberg-Access-Delegation", dm.protocolValue()));

    if (delegationMode.isEmpty()) {
      // Use local credentials on the client side
      propertiesBuilder.put("s3.access-key-id", MINIO_ACCESS_KEY);
      propertiesBuilder.put("s3.secret-access-key", MINIO_SECRET_KEY);
    }

    restCatalog.initialize("polaris", propertiesBuilder.buildKeepingLast());
    return restCatalog;
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(adminToken);
  }

  @ParameterizedTest
  @CsvSource("true,  true,")
  @CsvSource("false, true,")
  @CsvSource("true,  false,")
  @CsvSource("false, false,")
  public void testCreateTable(boolean pathStyle, boolean stsEnabled) throws IOException {
    LoadTableResponse response = doTestCreateTable(pathStyle, Optional.empty(), stsEnabled);
    assertThat(response.config()).doesNotContainKey(SECRET_ACCESS_KEY);
    assertThat(response.config()).doesNotContainKey(ACCESS_KEY_ID);
    assertThat(response.config()).doesNotContainKey(REFRESH_CREDENTIALS_ENDPOINT);
    assertThat(response.credentials()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreateTableVendedCredentials(boolean pathStyle) throws IOException {
    LoadTableResponse response =
        doTestCreateTable(pathStyle, Optional.of(VENDED_CREDENTIALS), true);
    assertThat(response.config())
        .containsEntry(
            REFRESH_CREDENTIALS_ENDPOINT,
            "v1/" + catalogName + "/namespaces/test-ns/tables/t1/credentials");
    assertThat(response.credentials()).hasSize(1);
  }

  private LoadTableResponse doTestCreateTable(
      boolean pathStyle, Optional<AccessDelegationMode> dm, boolean stsEnabled) throws IOException {
    try (RESTCatalog restCatalog =
        createCatalog(Optional.of(endpoint), Optional.empty(), pathStyle, dm, stsEnabled)) {
      LoadTableResponse loadTableResponse = doTestCreateTable(restCatalog, dm);
      if (pathStyle) {
        assertThat(loadTableResponse.config())
            .containsEntry("s3.path-style-access", Boolean.TRUE.toString());
      }
      return loadTableResponse;
    }
  }

  @Test
  public void testInternalEndpoints() throws IOException {
    try (RESTCatalog restCatalog =
        createCatalog(
            Optional.of("http://s3.example.com"),
            Optional.of(endpoint),
            false,
            Optional.of(endpoint),
            Optional.empty(),
            true)) {
      StorageConfigInfo storageConfig =
          managementApi.getCatalog(catalogName).getStorageConfigInfo();
      assertThat((AwsStorageConfigInfo) storageConfig)
          .extracting(
              AwsStorageConfigInfo::getEndpoint,
              AwsStorageConfigInfo::getStsEndpoint,
              AwsStorageConfigInfo::getEndpointInternal,
              AwsStorageConfigInfo::getPathStyleAccess)
          .containsExactly("http://s3.example.com", endpoint, endpoint, false);
      LoadTableResponse loadTableResponse = doTestCreateTable(restCatalog, Optional.empty());
      assertThat(loadTableResponse.config()).containsEntry(ENDPOINT, "http://s3.example.com");
    }
  }

  @Test
  public void testCreateTableFailureWithCredentialVendingWithoutSts() throws IOException {
    try (RESTCatalog restCatalog =
        createCatalog(
            Optional.of(endpoint),
            Optional.of("http://sts.example.com"), // not called
            false,
            Optional.of(VENDED_CREDENTIALS),
            false)) {
      StorageConfigInfo storageConfig =
          managementApi.getCatalog(catalogName).getStorageConfigInfo();
      assertThat((AwsStorageConfigInfo) storageConfig)
          .extracting(
              AwsStorageConfigInfo::getEndpoint,
              AwsStorageConfigInfo::getStsEndpoint,
              AwsStorageConfigInfo::getEndpointInternal,
              AwsStorageConfigInfo::getPathStyleAccess,
              AwsStorageConfigInfo::getStsUnavailable)
          .containsExactly(endpoint, "http://sts.example.com", null, false, true);

      catalogApi.createNamespace(catalogName, "test-ns");
      TableIdentifier id = TableIdentifier.of("test-ns", "t2");
      // Credential vending is not supported without STS
      assertThatThrownBy(() -> restCatalog.createTable(id, SCHEMA))
          .hasMessageContaining("but no credentials are available")
          .hasMessageContaining(id.toString());
    }
  }

  @Test
  public void testLoadTableFailureWithCredentialVendingWithoutSts() throws IOException {
    try (RESTCatalog restCatalog =
        createCatalog(
            Optional.of(endpoint),
            Optional.of("http://sts.example.com"), // not called
            false,
            Optional.empty(),
            false)) {

      catalogApi.createNamespace(catalogName, "test-ns");
      TableIdentifier id = TableIdentifier.of("test-ns", "t3");
      restCatalog.createTable(id, SCHEMA);

      // Credential vending is not supported without STS
      assertThatThrownBy(
              () ->
                  catalogApi.loadTable(
                      catalogName,
                      id,
                      "ALL",
                      Map.of("X-Iceberg-Access-Delegation", VENDED_CREDENTIALS.protocolValue())))
          .hasMessageContaining("but no credentials are available")
          .hasMessageContaining(id.toString());
    }
  }

  public LoadTableResponse doTestCreateTable(
      RESTCatalog restCatalog, Optional<AccessDelegationMode> dm) {
    catalogApi.createNamespace(catalogName, "test-ns");
    TableIdentifier id = TableIdentifier.of("test-ns", "t1");
    Table table = restCatalog.createTable(id, SCHEMA);
    assertThat(table).isNotNull();
    assertThat(restCatalog.tableExists(id)).isTrue();

    TableOperations ops = ((HasTableOperations) table).operations();
    URI location = URI.create(ops.current().metadataFileLocation());

    GetObjectResponse response =
        s3Client
            .getObject(
                GetObjectRequest.builder()
                    .bucket(location.getAuthority())
                    .key(location.getPath().substring(1)) // drop leading slash
                    .build())
            .response();
    assertThat(response.contentLength()).isGreaterThan(0);

    LoadTableResponse loadTableResponse =
        catalogApi.loadTable(
            catalogName,
            id,
            "ALL",
            dm.map(v -> Map.of("X-Iceberg-Access-Delegation", v.protocolValue())).orElse(Map.of()));

    assertThat(loadTableResponse.config()).containsKey(ENDPOINT);

    restCatalog.dropTable(id);
    assertThat(restCatalog.tableExists(id)).isFalse();
    return loadTableResponse;
  }

  @ParameterizedTest
  @CsvSource("true,  true,")
  @CsvSource("false, true,")
  @CsvSource("true,  false,")
  @CsvSource("false, false,")
  @CsvSource("true,  true,  VENDED_CREDENTIALS")
  @CsvSource("false, true,  VENDED_CREDENTIALS")
  public void testAppendFiles(
      boolean pathStyle, boolean stsEnabled, AccessDelegationMode delegationMode)
      throws IOException {
    try (RESTCatalog restCatalog =
        createCatalog(
            Optional.of(endpoint),
            Optional.of(endpoint),
            pathStyle,
            Optional.ofNullable(delegationMode),
            stsEnabled)) {
      catalogApi.createNamespace(catalogName, "test-ns");
      TableIdentifier id = TableIdentifier.of("test-ns", "t1");
      Table table = restCatalog.createTable(id, SCHEMA);
      assertThat(table).isNotNull();

      @SuppressWarnings("resource")
      FileIO io = table.io();

      URI loc =
          URI.create(
              table
                  .locationProvider()
                  .newDataLocation(
                      String.format(
                          "test-file-%s-%s-%s.txt", pathStyle, delegationMode, stsEnabled)));
      OutputFile f1 = io.newOutputFile(loc.toString());
      try (PositionOutputStream os = f1.create()) {
        os.write("Hello World".getBytes(UTF_8));
      }

      DataFile df =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(f1.location())
              .withFormat(FileFormat.PARQUET) // bogus value
              .withFileSizeInBytes(4)
              .withRecordCount(1)
              .build();

      table.newAppend().appendFile(df).commit();

      try (InputStream is =
          s3Client.getObject(
              GetObjectRequest.builder()
                  .bucket(loc.getAuthority())
                  .key(loc.getPath().substring(1)) // drop leading slash
                  .build())) {
        assertThat(new String(is.readAllBytes(), UTF_8)).isEqualTo("Hello World");
      }
    }
  }
}
