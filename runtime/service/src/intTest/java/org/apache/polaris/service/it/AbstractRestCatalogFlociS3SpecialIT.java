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
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ENDPOINT;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_KEY_ID;
import static org.apache.polaris.core.storage.StorageAccessProperty.AWS_SECRET_KEY;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.apache.polaris.test.commons.MinioRustProfile.ACCESS_KEY;
import static org.apache.polaris.test.commons.MinioRustProfile.SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
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
import org.apache.polaris.test.floci.aws.FlociAwsAccess;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

abstract class AbstractRestCatalogFlociS3SpecialIT {
  protected static final String BUCKET_URI_PREFIX = "/floci-s3-test";
  protected static final String TEST_REGION = "us-east-1";

  protected static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "doc"),
          optional(2, "data", Types.StringType.get()));

  private static String adminToken;

  protected static PolarisApiEndpoints endpoints;
  protected static PolarisClient client;
  protected static ManagementApi managementApi;
  protected static URI storageBase;
  protected static String endpoint;
  protected static S3Client s3Client;
  protected static String roleName;
  protected static String roleArn;

  protected CatalogApi catalogApi;
  protected String principalRoleName;
  protected PrincipalWithCredentials principalCredentials;
  protected String catalogName;

  protected abstract FlociAwsAccess flociAwsAccess();

  protected boolean kmsUnavailableForStsCatalogs() {
    return true;
  }

  @BeforeAll
  void setup(PolarisApiEndpoints apiEndpoints, ClientCredentials credentials) {
    var flociAws = flociAwsAccess();
    endpoints = apiEndpoints;
    s3Client = flociAws.s3Client();
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    storageBase = flociAws.s3BucketUri(BUCKET_URI_PREFIX);
    endpoint = flociAws.s3endpoint();
    roleName = "polaris-floci-access-role-" + UUID.randomUUID();
    roleArn = "arn:aws:iam::" + flociAws.accountId() + ":role/" + roleName;
    createRoleWithAccess(flociAws);
  }

  @AfterAll
  void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    var principalName = client.newEntityName("test-user");
    principalRoleName = client.newEntityName("test-admin");
    principalCredentials = managementApi.createPrincipalWithRole(principalName, principalRoleName);

    var principalToken = client.obtainToken(principalCredentials);
    catalogApi = client.catalogApi(principalToken);

    catalogName = client.newEntityName(testInfo.getTestMethod().orElseThrow().getName());
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(adminToken);
  }

  protected RESTCatalog createCatalog(
      Optional<String> endpoint,
      Optional<String> stsEndpoint,
      boolean pathStyleAccess,
      Optional<AccessDelegationMode> delegationMode,
      boolean stsEnabled) {
    return createCatalog(
        endpoint,
        stsEndpoint,
        pathStyleAccess,
        Optional.empty(),
        delegationMode,
        stsEnabled,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  protected RESTCatalog createCatalog(
      Optional<String> endpoint,
      Optional<String> stsEndpoint,
      boolean pathStyleAccess,
      Optional<String> endpointInternal,
      Optional<AccessDelegationMode> delegationMode,
      boolean stsEnabled,
      Optional<String> region,
      Optional<String> roleArn,
      Optional<Boolean> kmsUnavailable) {
    var storageConfigBuilder =
        AwsStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setPathStyleAccess(pathStyleAccess)
            .setStsUnavailable(!stsEnabled)
            .setAllowedLocations(List.of(storageBase.toASCIIString() + "/" + catalogName));

    endpoint.ifPresent(storageConfigBuilder::setEndpoint);
    stsEndpoint.ifPresent(storageConfigBuilder::setStsEndpoint);
    endpointInternal.ifPresent(storageConfigBuilder::setEndpointInternal);
    region.ifPresent(storageConfigBuilder::setRegion);
    roleArn.ifPresent(storageConfigBuilder::setRoleArn);
    kmsUnavailable.ifPresent(storageConfigBuilder::setKmsUnavailable);

    var catalogProps = CatalogProperties.builder(storageBase.toASCIIString() + "/" + catalogName);
    if (!stsEnabled) {
      catalogProps.addProperty(TABLE_DEFAULT_PREFIX + AWS_KEY_ID.getPropertyName(), ACCESS_KEY);
      catalogProps.addProperty(TABLE_DEFAULT_PREFIX + AWS_SECRET_KEY.getPropertyName(), SECRET_KEY);
    }
    var catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(storageConfigBuilder.build())
            .setProperties(catalogProps.build())
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    var authToken = client.obtainToken(principalCredentials);
    var restCatalog = new RESTCatalog();

    var propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(
                org.apache.iceberg.CatalogProperties.URI, endpoints.catalogApiEndpoint().toString())
            .put(OAuth2Properties.TOKEN, authToken)
            .put("warehouse", catalogName)
            .putAll(endpoints.extraHeaders("header."));

    delegationMode.ifPresent(
        dm -> propertiesBuilder.put("header.X-Iceberg-Access-Delegation", dm.protocolValue()));

    if (delegationMode.isEmpty()) {
      propertiesBuilder.put("s3.access-key-id", ACCESS_KEY);
      propertiesBuilder.put("s3.secret-access-key", SECRET_KEY);
    }

    restCatalog.initialize("polaris", propertiesBuilder.buildKeepingLast());
    return restCatalog;
  }

  protected LoadTableResponse doTestCreateTable(
      boolean pathStyle, Optional<AccessDelegationMode> dm, boolean stsEnabled) throws IOException {
    try (var restCatalog =
        createCatalog(
            Optional.of(endpoint),
            stsEnabled ? Optional.of(endpoint) : Optional.empty(),
            pathStyle,
            Optional.empty(),
            dm,
            stsEnabled,
            regionForSts(stsEnabled),
            roleArnForSts(stsEnabled),
            kmsUnavailableForSts(stsEnabled))) {
      var loadTableResponse = doTestCreateTable(restCatalog, dm);
      if (pathStyle) {
        assertThat(loadTableResponse.config())
            .containsEntry("s3.path-style-access", Boolean.TRUE.toString());
      }
      return loadTableResponse;
    }
  }

  protected Optional<String> regionForSts(boolean stsEnabled) {
    return stsEnabled ? Optional.of(TEST_REGION) : Optional.empty();
  }

  protected Optional<String> roleArnForSts(boolean stsEnabled) {
    return stsEnabled ? Optional.of(roleArn) : Optional.empty();
  }

  protected Optional<Boolean> kmsUnavailableForSts(boolean stsEnabled) {
    return stsEnabled ? Optional.of(kmsUnavailableForStsCatalogs()) : Optional.empty();
  }

  protected LoadTableResponse doTestCreateTable(
      RESTCatalog restCatalog, Optional<AccessDelegationMode> dm) {
    var tableIdentifier = createTableAndVerifyMetadata(restCatalog);
    assertThat(restCatalog.tableExists(tableIdentifier)).isTrue();

    try {
      var loadTableResponse =
          catalogApi.loadTable(
              catalogName,
              tableIdentifier,
              "ALL",
              dm.map(v -> Map.of("X-Iceberg-Access-Delegation", v.protocolValue()))
                  .orElse(Map.of()));

      assertThat(loadTableResponse.config()).containsKey(ENDPOINT);
      return loadTableResponse;
    } finally {
      if (restCatalog.tableExists(tableIdentifier)) {
        restCatalog.dropTable(tableIdentifier);
        assertThat(restCatalog.tableExists(tableIdentifier)).isFalse();
      }
    }
  }

  protected TableIdentifier createTableAndVerifyMetadata(RESTCatalog restCatalog) {
    catalogApi.createNamespace(catalogName, "test-ns");
    var tableIdentifier = TableIdentifier.of("test-ns", "t1");
    var table = restCatalog.createTable(tableIdentifier, SCHEMA);
    assertThat(table).isNotNull();

    var ops = ((HasTableOperations) table).operations();
    var location = URI.create(ops.current().metadataFileLocation());

    var response =
        s3Client
            .getObject(
                GetObjectRequest.builder()
                    .bucket(location.getAuthority())
                    .key(location.getPath().substring(1))
                    .build())
            .response();
    assertThat(response.contentLength()).isGreaterThan(0);
    return tableIdentifier;
  }

  protected void assertLoadTableWithVendedCredentialsSucceeds(TableIdentifier id) {
    var response =
        catalogApi.loadTable(
            catalogName,
            id,
            "ALL",
            Map.of("X-Iceberg-Access-Delegation", VENDED_CREDENTIALS.protocolValue()));
    assertThat(response.config())
        .containsEntry(
            REFRESH_CREDENTIALS_ENDPOINT,
            "v1/" + catalogName + "/namespaces/test-ns/tables/t1/credentials");
    assertThat(response.credentials()).hasSize(1);
  }

  protected String readS3Object(URI location) throws IOException {
    try (var is =
        s3Client.getObject(
            GetObjectRequest.builder()
                .bucket(location.getAuthority())
                .key(location.getPath().substring(1))
                .build())) {
      return new String(is.readAllBytes(), UTF_8);
    }
  }

  private static void createRoleWithAccess(FlociAwsAccess flociAws) {
    flociAws
        .iamClient()
        .createRole(b -> b.roleName(roleName).assumeRolePolicyDocument(trustPolicy(flociAws)));
    flociAws
        .iamClient()
        .putRolePolicy(
            b ->
                b.roleName(roleName)
                    .policyName("polaris-s3-kms-access")
                    .policyDocument(s3AndKmsPolicy(flociAws)));
  }

  private static String trustPolicy(FlociAwsAccess flociAws) {
    return """
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Principal": {
                "AWS": "arn:aws:iam::%s:root"
              },
              "Action": "sts:AssumeRole"
            }
          ]
        }
        """
        .formatted(flociAws.accountId());
  }

  private static String s3AndKmsPolicy(FlociAwsAccess flociAws) {
    return """
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
              ],
              "Resource": "arn:aws:s3:::%s/*"
            },
            {
              "Effect": "Allow",
              "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
              ],
              "Resource": "arn:aws:s3:::%s"
            },
            {
              "Effect": "Allow",
              "Action": [
                "kms:DescribeKey",
                "kms:Decrypt",
                "kms:Encrypt",
                "kms:GenerateDataKey",
                "kms:GenerateDataKeyWithoutPlaintext"
              ],
              "Resource": "arn:aws:kms:%s:%s:key/*"
            }
          ]
        }
        """
        .formatted(
            flociAws.bucket(),
            flociAws.bucket(),
            flociAws.region().orElse(TEST_REGION),
            flociAws.accountId());
  }
}
