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
import static org.apache.iceberg.aws.AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ACCESS_KEY_ID;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ENDPOINT;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.SECRET_ACCESS_KEY;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.apache.polaris.test.commons.MinioRustProfile.ACCESS_KEY;
import static org.apache.polaris.test.commons.MinioRustProfile.SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.test.commons.MinioRustProfile;
import org.apache.polaris.test.floci.aws.FlociAws;
import org.apache.polaris.test.floci.aws.FlociAwsAccess;
import org.apache.polaris.test.floci.aws.FlociAwsTestResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * These tests complement {@link PolarisRestCatalogFlociS3IT} to validate client-side access to
 * Floci S3 storage via {@code FileIO} instances configured from catalog's {@code loadTable}
 * responses with some S3-specific options.
 */
@QuarkusIntegrationTest
@TestProfile(MinioRustProfile.class)
@QuarkusTestResource(
    value = FlociAwsTestResource.class,
    initArgs = {
      @ResourceArg(name = "accessKey", value = ACCESS_KEY),
      @ResourceArg(name = "secretKey", value = SECRET_KEY),
      @ResourceArg(name = "bucket", value = "floci-s3-special-test"),
      @ResourceArg(name = "region", value = AbstractRestCatalogFlociS3SpecialIT.TEST_REGION),
      @ResourceArg(name = "iamEnforcement", value = "true")
    })
@ExtendWith(PolarisIntegrationTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestCatalogFlociS3SpecialIT extends AbstractRestCatalogFlociS3SpecialIT {

  @FlociAws static FlociAwsAccess flociAwsAccess;

  @Override
  protected FlociAwsAccess flociAwsAccess() {
    return flociAwsAccess;
  }

  @ParameterizedTest
  @CsvSource({"true,  true,", "false, true,", "true,  false,", "false, false,"})
  public void testCreateTable(boolean pathStyle, boolean stsEnabled) throws IOException {
    var response = doTestCreateTable(pathStyle, Optional.empty(), stsEnabled);
    assertThat(response.config()).doesNotContainKey(SECRET_ACCESS_KEY);
    assertThat(response.config()).doesNotContainKey(ACCESS_KEY_ID);
    assertThat(response.config()).doesNotContainKey(REFRESH_CREDENTIALS_ENDPOINT);
    assertThat(response.credentials()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreateTableVendedCredentials(boolean pathStyle) throws IOException {
    var response = doTestCreateTable(pathStyle, Optional.of(VENDED_CREDENTIALS), true);
    assertThat(response.config())
        .containsEntry(
            REFRESH_CREDENTIALS_ENDPOINT,
            "v1/" + catalogName + "/namespaces/test-ns/tables/t1/credentials");
    assertThat(response.credentials()).hasSize(1);
  }

  @Test
  public void testCreateTableVendedCredentialsWithoutRegionPasses() throws IOException {
    try (var restCatalog =
        createCatalog(
            Optional.of(endpoint),
            Optional.of(endpoint),
            true,
            Optional.empty(),
            Optional.of(VENDED_CREDENTIALS),
            true,
            Optional.empty(),
            Optional.of(roleArn),
            Optional.of(kmsUnavailableForStsCatalogs()))) {
      var id = createTableAndVerifyMetadata(restCatalog);
      try {
        assertLoadTableWithVendedCredentialsSucceeds(id);
      } finally {
        catalogApi.dropTable(catalogName, id);
      }
    }
  }

  @Test
  public void testCreateTableVendedCredentialsWithFullAwsShapePasses() throws IOException {
    try (var restCatalog =
        createCatalog(
            Optional.of(endpoint),
            Optional.of(endpoint),
            true,
            Optional.empty(),
            Optional.of(VENDED_CREDENTIALS),
            true,
            Optional.of(TEST_REGION),
            Optional.of(roleArn),
            Optional.of(kmsUnavailableForStsCatalogs()))) {
      var id = createTableAndVerifyMetadata(restCatalog);
      try {
        assertLoadTableWithVendedCredentialsSucceeds(id);
      } finally {
        catalogApi.dropTable(catalogName, id);
      }
    }
  }

  @Test
  public void testInternalEndpoints() throws IOException {
    try (var restCatalog =
        createCatalog(
            Optional.of("http://s3.example.com"),
            Optional.of(endpoint),
            false,
            Optional.of(endpoint),
            Optional.empty(),
            true,
            regionForSts(true),
            roleArnForSts(true),
            kmsUnavailableForSts(true))) {
      var storageConfig = managementApi.getCatalog(catalogName).getStorageConfigInfo();
      assertThat((AwsStorageConfigInfo) storageConfig)
          .extracting(
              AwsStorageConfigInfo::getEndpoint,
              AwsStorageConfigInfo::getStsEndpoint,
              AwsStorageConfigInfo::getEndpointInternal,
              AwsStorageConfigInfo::getPathStyleAccess)
          .containsExactly("http://s3.example.com", endpoint, endpoint, false);
      var loadTableResponse = doTestCreateTable(restCatalog, Optional.empty());
      assertThat(loadTableResponse.config()).containsEntry(ENDPOINT, "http://s3.example.com");
    }
  }

  @Test
  public void testCreateTableFailureWithCredentialVendingWithoutSts() throws IOException {
    try (var restCatalog =
        createCatalog(
            Optional.of(endpoint),
            Optional.of("http://sts.example.com"),
            false,
            Optional.of(VENDED_CREDENTIALS),
            false)) {
      var storageConfig = managementApi.getCatalog(catalogName).getStorageConfigInfo();
      assertThat((AwsStorageConfigInfo) storageConfig)
          .extracting(
              AwsStorageConfigInfo::getEndpoint,
              AwsStorageConfigInfo::getStsEndpoint,
              AwsStorageConfigInfo::getEndpointInternal,
              AwsStorageConfigInfo::getPathStyleAccess,
              AwsStorageConfigInfo::getStsUnavailable)
          .containsExactly(endpoint, "http://sts.example.com", null, false, true);

      catalogApi.createNamespace(catalogName, "test-ns");
      var id = TableIdentifier.of("test-ns", "t2");
      assertThatThrownBy(() -> restCatalog.createTable(id, SCHEMA))
          .hasMessageContaining("but no credentials are available")
          .hasMessageContaining(id.toString());
    }
  }

  @Test
  public void testLoadTableFailureWithCredentialVendingWithoutSts() throws IOException {
    try (var restCatalog =
        createCatalog(
            Optional.of(endpoint),
            Optional.of("http://sts.example.com"),
            false,
            Optional.empty(),
            false)) {

      catalogApi.createNamespace(catalogName, "test-ns");
      var id = TableIdentifier.of("test-ns", "t3");
      restCatalog.createTable(id, SCHEMA);

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

  @ParameterizedTest
  @CsvSource({
    "true,  true,",
    "false, true,",
    "true,  false,",
    "false, false,",
    "true,  true,  VENDED_CREDENTIALS",
    "false, true,  VENDED_CREDENTIALS"
  })
  public void testAppendFiles(
      boolean pathStyle, boolean stsEnabled, AccessDelegationMode delegationMode)
      throws IOException {
    try (var restCatalog =
        createCatalog(
            Optional.of(endpoint),
            Optional.of(endpoint),
            pathStyle,
            Optional.empty(),
            Optional.ofNullable(delegationMode),
            stsEnabled,
            regionForSts(stsEnabled),
            roleArnForSts(stsEnabled),
            kmsUnavailableForSts(stsEnabled))) {
      catalogApi.createNamespace(catalogName, "test-ns");
      var id = TableIdentifier.of("test-ns", "t1");
      var table = restCatalog.createTable(id, SCHEMA);
      assertThat(table).isNotNull();

      @SuppressWarnings("resource")
      var io = table.io();

      var loc =
          URI.create(
              table
                  .locationProvider()
                  .newDataLocation(
                      String.format(
                          "test-file-%s-%s-%s.txt", pathStyle, delegationMode, stsEnabled)));
      var f1 = io.newOutputFile(loc.toString());
      try (var os = f1.create()) {
        os.write("Hello World".getBytes(UTF_8));
      }

      var df =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(f1.location())
              .withFormat(FileFormat.PARQUET)
              .withFileSizeInBytes(4)
              .withRecordCount(1)
              .build();

      table.newAppend().appendFile(df).commit();

      assertThat(readS3Object(loc)).isEqualTo("Hello World");
    }
  }
}
