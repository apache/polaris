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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class AwsEndpointCatalogConfigValidatorTest {

  private static final String S3_ENDPOINT = "https://s3.us-west-2.amazonaws.com";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://sts.us-west-2.amazonaws.com",
        "http://sts.example.com:9000",
        "https://user@sts.example.com",
        "https://sts.example.com/?token=1",
        "https://sts.example.com/#fragment"
      })
  void validSigV4StsEndpointPasses(String stsEndpoint) {
    AwsEndpointCatalogConfigValidator validator = validator(true, true);

    assertThatCode(() -> validator.validate(sigV4Catalog(stsEndpoint))).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @MethodSource("invalidStorageEndpoints")
  void invalidAwsStorageEndpointRejected(String fieldName, AwsStorageConfigInfo storage) {
    AwsEndpointCatalogConfigValidator validator = validator(true, true);

    assertThatThrownBy(() -> validator.validate(awsCatalog(storage)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(fieldName);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "sts.us-west-2.amazonaws.com",
        "ftp://sts.example.com",
        "file:///tmp/sts",
        "",
        "not a uri"
      })
  void invalidSigV4StsEndpointRejected(String stsEndpoint) {
    AwsEndpointCatalogConfigValidator validator = validator(true, true);

    assertThatThrownBy(() -> validator.validate(sigV4Catalog(stsEndpoint)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("authenticationParameters.stsEndpoint");
  }

  @ParameterizedTest
  @MethodSource("storageWithS3Endpoint")
  void s3EndpointsRejectedOnEveryValidationWhenS3EndpointsDisabled(AwsStorageConfigInfo storage) {
    AwsEndpointCatalogConfigValidator validator = validator(false, true);

    assertThatThrownBy(() -> validator.validate(awsCatalog(storage)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "This catalog sets custom S3 endpoints, which the realm configuration does not allow.");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void stsDisabledRejectedWhenS3EndpointsDisabled(boolean stsUnavailable) {
    AwsEndpointCatalogConfigValidator validator = validator(false, true);
    AwsStorageConfigInfo storage =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/polaris-test")
            .setAllowedLocations(List.of("s3://bucket/base/"))
            .setStsUnavailable(stsUnavailable)
            .build();

    assertThatThrownBy(() -> validator.validate(awsCatalog(storage)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("This catalog disables STS, which the realm configuration does not allow.");
  }

  @Test
  void awsStorageWithoutEndpointsPassesWhenS3EndpointsDisabled() {
    AwsEndpointCatalogConfigValidator validator = validator(false, true);

    assertThatCode(() -> validator.validate(awsCatalog(awsStorage(null, null, null))))
        .doesNotThrowAnyException();
  }

  @Test
  void validAwsStorageEndpointsPass() {
    AwsEndpointCatalogConfigValidator validator = validator(true, true);

    assertThatCode(() ->
            validator.validate(
                awsCatalog(
                    awsStorage(
                        "http://minio.example.com:9000",
                        "https://minio-internal.example.com",
                        "https://sts.example.com"))))
        .doesNotThrowAnyException();
  }

  @Test
  void sigV4StsEndpointRejectedWhenS3EndpointsDisabled() {
    AwsEndpointCatalogConfigValidator validator = validator(false, true);

    assertThatThrownBy(() -> validator.validate(sigV4Catalog(S3_ENDPOINT)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "This catalog sets a custom SigV4 STS endpoint, "
                + "which the realm configuration does not allow.");
  }

  @Test
  void sigV4WithoutStsEndpointPassesWhenS3EndpointsDisabled() {
    AwsEndpointCatalogConfigValidator validator = validator(false, true);

    assertThatCode(() -> validator.validate(sigV4Catalog(null))).doesNotThrowAnyException();
  }

  @Test
  void uriChecksAreSkippedWhenValidateAwsEndpointUrisDisabled() {
    AwsEndpointCatalogConfigValidator validator = validator(true, false);

    assertThatCode(() ->
            validator.validate(
                awsCatalog(awsStorage("ftp://s3.example.com", "not a uri", "file:///tmp"))))
        .doesNotThrowAnyException();
    assertThatCode(() -> validator.validate(sigV4Catalog("not a uri"))).doesNotThrowAnyException();
  }

  @Test
  void s3EndpointsFlagStillRejectsSigV4StsEndpointWhenUriChecksDisabled() {
    AwsEndpointCatalogConfigValidator validator = validator(false, false);

    assertThatThrownBy(() -> validator.validate(sigV4Catalog(S3_ENDPOINT)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "This catalog sets a custom SigV4 STS endpoint, "
                + "which the realm configuration does not allow.");
  }

  @Test
  void internalCatalogWithoutEndpointsPasses() {
    AwsEndpointCatalogConfigValidator validator = validator(true, true);

    assertThatCode(() -> validator.validate(awsCatalog(awsStorage(null, null, null))))
        .doesNotThrowAnyException();
  }

  @Test
  void nonAwsStorageIsNotEndpointValidated() {
    AwsEndpointCatalogConfigValidator validator = validator(true, true);
    FileStorageConfigInfo fileStorage =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://tmp"))
            .build();

    assertThatCode(() ->
            validator.validate(
                PolarisCatalog.builder()
                    .setType(Catalog.TypeEnum.INTERNAL)
                    .setName("file-catalog")
                    .setProperties(new CatalogProperties("file://tmp"))
                    .setStorageConfigInfo(fileStorage)
                    .build()))
        .doesNotThrowAnyException();
  }

  static Stream<AwsStorageConfigInfo> storageWithS3Endpoint() {
    return Stream.of(
        awsStorage(S3_ENDPOINT, null, null),
        awsStorage(null, S3_ENDPOINT, null),
        awsStorage(null, null, S3_ENDPOINT));
  }

  static Stream<Arguments> invalidStorageEndpoints() {
    List<String> invalidValues =
        List.of(
            "sts.us-west-2.amazonaws.com",
            "ftp://s3.example.com",
            "file:///tmp/s3",
            "",
            "not a uri");
    return invalidValues.stream()
        .flatMap(value ->
            Stream.of(
                Arguments.of("storageConfigInfo.endpoint", awsStorage(value, null, null)),
                Arguments.of("storageConfigInfo.endpointInternal", awsStorage(null, value, null)),
                Arguments.of("storageConfigInfo.stsEndpoint", awsStorage(null, null, value))));
  }

  private static AwsEndpointCatalogConfigValidator validator(
      boolean allowS3Endpoints, boolean validateUris) {
    RealmConfig realmConfig =
        TestServices.builder()
            .config(
                Map.of(
                    FeatureConfiguration.ALLOW_SETTING_S3_ENDPOINTS.key(),
                    allowS3Endpoints,
                    FeatureConfiguration.VALIDATE_AWS_ENDPOINT_URIS.key(),
                    validateUris))
            .build()
            .realmConfig();
    return new AwsEndpointCatalogConfigValidator(realmConfig);
  }

  private static AwsStorageConfigInfo awsStorage(
      String endpoint, String endpointInternal, String stsEndpoint) {
    AwsStorageConfigInfo.Builder builder =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setRoleArn("arn:aws:iam::123456789012:role/polaris-test")
            .setAllowedLocations(List.of("s3://bucket/base/"));
    if (endpoint != null) {
      builder.setEndpoint(endpoint);
    }
    if (endpointInternal != null) {
      builder.setEndpointInternal(endpointInternal);
    }
    if (stsEndpoint != null) {
      builder.setStsEndpoint(stsEndpoint);
    }
    return builder.build();
  }

  private static Catalog awsCatalog(AwsStorageConfigInfo storage) {
    return PolarisCatalog.builder()
        .setType(Catalog.TypeEnum.INTERNAL)
        .setName("aws-catalog")
        .setProperties(new CatalogProperties("s3://bucket/base/"))
        .setStorageConfigInfo(storage)
        .build();
  }

  private static Catalog sigV4Catalog(String stsEndpoint) {
    SigV4AuthenticationParameters authenticationParameters =
        SigV4AuthenticationParameters.builder()
            .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
            .setRoleArn("arn:aws:iam::123456789012:role/customer-role")
            .setSigningRegion("us-west-2")
            .setStsEndpoint(stsEndpoint)
            .build();
    ConnectionConfigInfo connectionConfigInfo = Mockito.mock(ConnectionConfigInfo.class);
    Mockito.when(connectionConfigInfo.getAuthenticationParameters())
        .thenReturn(authenticationParameters);

    return ExternalCatalog.builder()
        .setType(Catalog.TypeEnum.EXTERNAL)
        .setName("sigv4-catalog")
        .setProperties(new CatalogProperties("s3://bucket/base/"))
        .setStorageConfigInfo(awsStorage(null, null, null))
        .setConnectionConfigInfo(connectionConfigInfo)
        .build();
  }
}
