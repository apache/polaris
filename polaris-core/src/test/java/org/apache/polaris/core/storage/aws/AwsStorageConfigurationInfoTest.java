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

import java.net.URI;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AwsStorageConfigurationInfoTest {

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
        Arguments.of("urn:ecs:sts::s3:assumed-role/s3assumeRole/user1-105-temp", "s3", "ecs"),
        Arguments.of("urn:sgws:identity::12345:group/foo-bar-abcdef", "12345", "sgws"));
  }
}
