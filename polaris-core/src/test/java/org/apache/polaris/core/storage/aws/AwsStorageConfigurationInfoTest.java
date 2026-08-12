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

package org.apache.polaris.core.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AwsStorageConfigurationInfoTest {

  private static final String ALLOWED_KMS_KEY_ARN =
      "arn:aws:kms:us-east-1:012345678901:key/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
  private static final String DECRYPT_ONLY_KMS_KEY_ARN =
      "arn:aws:kms:us-east-1:012345678901:key/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

  @Test
  public void testStsEndpoint() {
    assertThat(newBuilder().build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().stsEndpoint("http://sts.example.com").build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(null, URI.create("http://sts.example.com"));
    assertThat(newBuilder().endpoint("http://s3.example.com").build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://s3.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://s3.example.com")
                .stsEndpoint("http://sts.example.com")
                .build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://sts.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://s3.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(
            URI.create("http://s3.example.com"),
            URI.create("http://int.example.com"),
            URI.create("http://int.example.com"));
  }

  private static ImmutableAwsStorageConfigurationInfo.Builder newBuilder() {
    return AwsStorageConfigurationInfo.builder()
        .roleARN("arn:aws:iam::123456789012:role/polaris-test");
  }

  @Test
  public void testInternalEndpoint() {
    assertThat(newBuilder().build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().stsEndpoint("http://sts.example.com").build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().endpoint("http://s3.example.com").build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://s3.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://s3.example.com")
                .stsEndpoint("http://sts.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://int.example.com"));
    assertThat(
            newBuilder()
                .stsEndpoint("http://sts.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, URI.create("http://int.example.com"));
  }

  @Test
  public void testPathStyleAccess() {
    assertThat(newBuilder().pathStyleAccess(null).build().getPathStyleAccess()).isNull();
    assertThat(newBuilder().pathStyleAccess(false).build().getPathStyleAccess()).isFalse();
    assertThat(newBuilder().pathStyleAccess(true).build().getPathStyleAccess()).isTrue();
  }

  @Test
  public void testStsUnavailable() {
    assertThat(newBuilder().build().getStsUnavailable()).isNull();
    assertThat(newBuilder().stsUnavailable(null).build().getStsUnavailable()).isNull();
    assertThat(newBuilder().stsUnavailable(false).build().getStsUnavailable()).isFalse();
    assertThat(newBuilder().stsUnavailable(true).build().getStsUnavailable()).isTrue();
  }

  @Test
  public void testKmsUnavailable() {
    assertThat(newBuilder().build().getKmsUnavailable()).isNull();
    assertThat(newBuilder().kmsUnavailable(null).build().getKmsUnavailable()).isNull();
    assertThat(newBuilder().kmsUnavailable(false).build().getKmsUnavailable()).isFalse();
    assertThat(newBuilder().kmsUnavailable(true).build().getKmsUnavailable()).isTrue();
  }

  @Test
  public void testDecryptOnlyKmsKeysMustNotOverlapEncryptCapableKeys() {
    String keyArn = "arn:aws:kms:us-east-1:012345678901:key/cccccccc-cccc-cccc-cccc-cccccccccccc";

    assertThatThrownBy(
            () -> newBuilder().currentKmsKey(keyArn).decryptOnlyKmsKeys(List.of(keyArn)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("currentKmsKey must not also be configured in decryptOnlyKmsKeys");

    assertThatThrownBy(
            () ->
                newBuilder()
                    .allowedKmsKeys(List.of(keyArn))
                    .decryptOnlyKmsKeys(List.of(keyArn))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("allowedKmsKeys and decryptOnlyKmsKeys must not overlap");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidKmsKeyArns")
  public void testKmsKeysRejectNonCanonicalArnsWhenDecryptOnlyKmsKeysConfigured(
      String description, String invalidKeyArn) {
    assertThatThrownBy(() -> newBuilder().decryptOnlyKmsKeys(List.of(invalidKeyArn)).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("decryptOnlyKmsKeys must contain concrete AWS KMS key ARNs");

    assertThatThrownBy(
            () ->
                newBuilder()
                    .allowedKmsKeys(List.of(invalidKeyArn))
                    .decryptOnlyKmsKeys(List.of(DECRYPT_ONLY_KMS_KEY_ARN))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("allowedKmsKeys must contain concrete AWS KMS key ARNs");

    assertThatThrownBy(
            () ->
                newBuilder()
                    .currentKmsKey(invalidKeyArn)
                    .decryptOnlyKmsKeys(List.of(DECRYPT_ONLY_KMS_KEY_ARN))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("currentKmsKey must contain concrete AWS KMS key ARNs");
  }

  static Stream<Arguments> invalidKmsKeyArns() {
    return Stream.of(
        Arguments.of("alias ARN", "arn:aws:kms:us-east-1:012345678901:alias/example-key"),
        Arguments.of("wildcard key ARN", "arn:aws:kms:us-east-1:012345678901:key/*"),
        Arguments.of(
            "partial wildcard key ARN", "arn:aws:kms:us-east-1:012345678901:key/1234abcd-*"),
        Arguments.of("bare key ID", "1234abcd-12ab-34cd-56ef-1234567890ab"),
        Arguments.of(
            "missing region",
            "arn:aws:kms::012345678901:key/1234abcd-12ab-34cd-56ef-1234567890ab"));
  }

  @ParameterizedTest
  @MethodSource("validKmsKeyArns")
  public void testDecryptOnlyKmsKeysAcceptConcreteKeyArns(String keyArn) {
    assertThat(
            newBuilder()
                .allowedKmsKeys(List.of(ALLOWED_KMS_KEY_ARN))
                .decryptOnlyKmsKeys(List.of(keyArn))
                .build()
                .getDecryptOnlyKmsKeys())
        .containsExactly(keyArn);
  }

  static Stream<String> validKmsKeyArns() {
    return Stream.of(
        DECRYPT_ONLY_KMS_KEY_ARN,
        "arn:aws:kms:us-east-1:012345678901:key/mrk-1234abcd12ab34cd56ef1234567890ab",
        "arn:aws-us-gov:kms:us-gov-west-1:012345678901:key/1234abcd-12ab-34cd-56ef-1234567890ab");
  }

  @ParameterizedTest
  @MethodSource
  public void testRoleArnParsing(
      String roleArn, String expectedAccountId, String expectedPartition) {
    AwsStorageConfigurationInfo awsConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://bucket/path/to/warehouse")
            .roleARN(roleArn)
            .region("us-east-2")
            .build();

    Assertions.assertThat(awsConfig)
        .extracting(
            AwsStorageConfigurationInfo::getRoleARN,
            AwsStorageConfigurationInfo::getAwsAccountId,
            AwsStorageConfigurationInfo::getAwsPartition)
        .containsExactly(roleArn, expectedAccountId, expectedPartition);
  }

  static Stream<Arguments> testRoleArnParsing() {
    return Stream.of(
        Arguments.of("arn:aws:iam::012345678901:role/jdoe", "012345678901", "aws"),
        Arguments.of("arn:aws-us-gov:iam::012345678901:role/jdoe", "012345678901", "aws-us-gov"),
        Arguments.of("arn:aws-cn:iam::012345678901:role/jdoe", "012345678901", "aws-cn"),
        // Following are only there to have test coverage for non-AWS role-ARNs/URNs. ARN specific
        // parts are irrelevant for those URNs.
        Arguments.of(
            "urn:ecs:sts::namespace:assumed-role/s3assumeRole/user1-105-temp", "namespace", "ecs"),
        Arguments.of("urn:sgws:identity::12345:group/foo-bar-abcdef", "12345", "sgws"));
  }
}
