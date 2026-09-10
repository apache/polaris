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
package org.apache.polaris.service.catalog.validation;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.FileStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.S3CredentialIssuer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IcebergPropertiesValidationTest {

  @Mock private RealmConfig realmConfig;

  private void allow(String... issuers) {
    when(realmConfig.getConfig(eq(FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_ISSUERS)))
        .thenReturn(List.of(issuers));
  }

  @Test
  void defaultAllowlistIsStsOnly() {
    when(realmConfig.getConfig(eq(FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_ISSUERS)))
        .thenReturn(FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_ISSUERS.defaultValue());
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialIssuerAllowed(
                    realmConfig, S3CredentialIssuer.STS))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialIssuerAllowed(
                    realmConfig, S3CredentialIssuer.CLOUDFLARE_R2))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential issuer CLOUDFLARE_R2 is not enabled in this realm");
  }

  @Test
  void theListHasNoImplicitMember() {
    allow("CLOUDFLARE_R2");
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialIssuerAllowed(
                    realmConfig, S3CredentialIssuer.STS))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential issuer STS is not enabled in this realm");
  }

  @Test
  void availabilityRejectsCloudflareR2InThisBuildEvenWhenAllowed() {
    allow("STS", "CLOUDFLARE_R2");
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialIssuerAvailable(
                    realmConfig, S3CredentialIssuer.STS))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialIssuerAvailable(
                    realmConfig, S3CredentialIssuer.CLOUDFLARE_R2))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential issuer CLOUDFLARE_R2 is not available in this build");
  }

  @Test
  void availabilityChecksTheAllowlistFirst() {
    allow("STS");
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialIssuerAvailable(
                    realmConfig, S3CredentialIssuer.CLOUDFLARE_R2))
        .hasMessage("S3 credential issuer CLOUDFLARE_R2 is not enabled in this realm");
  }

  @Test
  void storageConfigOverloadIgnoresNonS3AndNull() {
    allow("CLOUDFLARE_R2"); // would reject STS if consulted
    FileStorageConfigurationInfo file =
        FileStorageConfigurationInfo.builder().addAllowedLocation("file:///tmp/x/").build();
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialIssuerAvailable(realmConfig, file))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialIssuerAvailable(
                    realmConfig, (PolarisStorageConfigurationInfo) null))
        .doesNotThrowAnyException();
  }

  @Test
  void storageConfigOverloadReadsTheIssuerFromAnS3Config() {
    allow("STS");
    AwsStorageConfigurationInfo r2 =
        AwsStorageConfigurationInfo.builder()
            .credentialIssuer(S3CredentialIssuer.CLOUDFLARE_R2)
            .endpoint("https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com")
            .pathStyleAccess(true)
            .region("auto")
            .addAllowedLocation("s3://b/p/")
            .build();
    assertThatThrownBy(
            () -> IcebergPropertiesValidation.validateS3CredentialIssuerAvailable(realmConfig, r2))
        .hasMessage("S3 credential issuer CLOUDFLARE_R2 is not enabled in this realm");
  }
}
