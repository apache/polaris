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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.FileStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.service.storage.S3CredentialVendingMechanisms;
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

  private void allow(String... mechanisms) {
    when(realmConfig.getConfig(eq(FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS)))
        .thenReturn(List.of(mechanisms));
  }

  private static S3CredentialVendingMechanisms installed(String... ids) {
    Map<String, S3CredentialVendingMechanism> mechanisms = new HashMap<>();
    for (String id : ids) {
      mechanisms.put(id, mock(S3CredentialVendingMechanism.class));
    }
    return new S3CredentialVendingMechanisms(mechanisms);
  }

  @Test
  void defaultAllowlistIsStsOnly() {
    when(realmConfig.getConfig(eq(FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS)))
        .thenReturn(FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS.defaultValue());
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(
                    realmConfig, "STS"))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(
                    realmConfig, "SECOND_MECHANISM"))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "S3 credential vending mechanism SECOND_MECHANISM is not enabled in this realm");
  }

  @Test
  void theListHasNoImplicitMember() {
    allow("SECOND_MECHANISM");
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(
                    realmConfig, "STS"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential vending mechanism STS is not enabled in this realm");
  }

  @Test
  void mechanismCheckAllowsAnAllowlistedInstalledMechanism() {
    allow("STS", "SECOND_MECHANISM");
    S3CredentialVendingMechanisms mechanisms = installed("STS", "SECOND_MECHANISM", "DEFAULT");
    AwsStorageConfigurationInfo sts =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::123456789012:role/r")
            .addAllowedLocation("s3://b/p/")
            .build();
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanism(
                    realmConfig, sts, mechanisms))
        .doesNotThrowAnyException();
  }

  @Test
  void mechanismCheckRejectsAnAllowlistedButUninstalledMechanism() {
    allow("STS", "SECOND_MECHANISM");
    S3CredentialVendingMechanisms mechanisms = installed("STS");
    AwsStorageConfigurationInfo secondMechanism = secondMechanismConfig();
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanism(
                    realmConfig, secondMechanism, mechanisms))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "S3 credential vending mechanism SECOND_MECHANISM is not available in this server");
  }

  @Test
  void mechanismCheckChecksTheAllowlistFirst() {
    allow("STS");
    // Installed in this server, but not allowlisted in this realm: still refused, and with the
    // realm's message, not the registry's.
    S3CredentialVendingMechanisms mechanisms = installed("STS", "SECOND_MECHANISM");
    AwsStorageConfigurationInfo secondMechanism = secondMechanismConfig();
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanism(
                    realmConfig, secondMechanism, mechanisms))
        .hasMessage(
            "S3 credential vending mechanism SECOND_MECHANISM is not enabled in this realm");
  }

  @Test
  void anEmptyMechanismSkipsTheAllowlistAndResolvesToDefault() {
    allow("SECOND_MECHANISM"); // would refuse STS and DEFAULT if consulted
    AwsStorageConfigurationInfo empty =
        AwsStorageConfigurationInfo.builder().addAllowedLocation("s3://bucket/prefix/").build();
    IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(
        realmConfig, (String) null);
    IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(realmConfig, empty);
    IcebergPropertiesValidation.validateS3CredentialVendingMechanism(
        realmConfig, empty, installed("STS", "DEFAULT"));
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanism(
                    realmConfig, empty, installed("STS")))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential vending mechanism DEFAULT is not available in this server");
  }

  @Test
  void storageConfigOverloadsIgnoreNonS3AndNull() {
    allow("SECOND_MECHANISM"); // would reject STS if consulted
    S3CredentialVendingMechanisms mechanisms = installed("SECOND_MECHANISM");
    FileStorageConfigurationInfo file =
        FileStorageConfigurationInfo.builder().addAllowedLocation("file:///tmp/x/").build();
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(
                    realmConfig, file))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(
                    realmConfig, (PolarisStorageConfigurationInfo) null))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanism(
                    realmConfig, file, mechanisms))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanism(
                    realmConfig, (PolarisStorageConfigurationInfo) null, mechanisms))
        .doesNotThrowAnyException();
  }

  @Test
  void storageConfigOverloadReadsTheMechanismFromAnS3Config() {
    allow("STS");
    AwsStorageConfigurationInfo secondMechanism = secondMechanismConfig();
    assertThatThrownBy(
            () ->
                IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(
                    realmConfig, secondMechanism))
        .hasMessage(
            "S3 credential vending mechanism SECOND_MECHANISM is not enabled in this realm");
  }

  private static AwsStorageConfigurationInfo secondMechanismConfig() {
    return AwsStorageConfigurationInfo.builder()
        .credentialVendingMechanism("SECOND_MECHANISM")
        .addAllowedLocation("s3://bucket/prefix/")
        .build();
  }
}
