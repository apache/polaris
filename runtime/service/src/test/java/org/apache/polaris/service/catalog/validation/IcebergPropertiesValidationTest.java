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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
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
  void anEmptyMechanismSkipsTheAllowlistAndResolvesToDefault() {
    allow("SECOND_MECHANISM"); // would refuse STS and DEFAULT if consulted
    AwsStorageConfigurationInfo empty =
        AwsStorageConfigurationInfo.builder().addAllowedLocation("s3://bucket/prefix/").build();
    IcebergPropertiesValidation.validateS3CredentialVendingMechanismAllowed(
        realmConfig, (String) null);
    assertThat(empty.resolvedCredentialVendingMechanism())
        .isEqualTo(S3CredentialVendingMechanism.DEFAULT);
  }
}
