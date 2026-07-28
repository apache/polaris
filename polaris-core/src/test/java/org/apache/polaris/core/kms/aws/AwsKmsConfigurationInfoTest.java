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

package org.apache.polaris.core.kms.aws;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AwsKmsConfigurationInfoTest {

  @Test
  public void testStsEndpoint() {
    assertThat(newBuilder().build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri, AwsKmsConfigurationInfo::getStsEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().stsEndpoint("http://sts.example.com").build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri, AwsKmsConfigurationInfo::getStsEndpointUri)
        .containsExactly(null, URI.create("http://sts.example.com"));
    assertThat(newBuilder().endpoint("http://kms.example.com").build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri, AwsKmsConfigurationInfo::getStsEndpointUri)
        .containsExactly(
            URI.create("http://kms.example.com"), URI.create("http://kms.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://kms.example.com")
                .stsEndpoint("http://sts.example.com")
                .build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri, AwsKmsConfigurationInfo::getStsEndpointUri)
        .containsExactly(
            URI.create("http://kms.example.com"), URI.create("http://sts.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://kms.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri,
            AwsKmsConfigurationInfo::getStsEndpointUri,
            AwsKmsConfigurationInfo::getInternalEndpointUri)
        .containsExactly(
            URI.create("http://kms.example.com"),
            URI.create("http://int.example.com"),
            URI.create("http://int.example.com"));
  }

  @Test
  public void testInternalEndpoint() {
    assertThat(newBuilder().build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri,
            AwsKmsConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().stsEndpoint("http://sts.example.com").build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri,
            AwsKmsConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().endpoint("http://kms.example.com").build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri,
            AwsKmsConfigurationInfo::getInternalEndpointUri)
        .containsExactly(
            URI.create("http://kms.example.com"), URI.create("http://kms.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://kms.example.com")
                .stsEndpoint("http://sts.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri,
            AwsKmsConfigurationInfo::getInternalEndpointUri)
        .containsExactly(
            URI.create("http://kms.example.com"), URI.create("http://int.example.com"));
    assertThat(
            newBuilder()
                .stsEndpoint("http://sts.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsKmsConfigurationInfo::getEndpointUri,
            AwsKmsConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, URI.create("http://int.example.com"));
  }

  @ParameterizedTest
  @MethodSource
  public void testRoleArnParsing(
      String roleArn, String expectedAccountId, String expectedPartition) {
    AwsKmsConfigurationInfo awsConfig =
        AwsKmsConfigurationInfo.builder().roleArn(roleArn).region("us-east-2").build();

    Assertions.assertThat(awsConfig)
        .extracting(
            AwsKmsConfigurationInfo::getRoleArn,
            AwsKmsConfigurationInfo::getAwsAccountId,
            AwsKmsConfigurationInfo::getAwsPartition)
        .containsExactly(roleArn, expectedAccountId, expectedPartition);
  }

  static Stream<Arguments> testRoleArnParsing() {
    return Stream.of(
        Arguments.of("arn:aws:iam::012345678901:role/jdoe", "012345678901", "aws"),
        Arguments.of("arn:aws-us-gov:iam::012345678901:role/jdoe", "012345678901", "aws-us-gov"),
        Arguments.of("arn:aws-cn:iam::012345678901:role/jdoe", "012345678901", "aws-cn"));
  }

  private static ImmutableAwsKmsConfigurationInfo.Builder newBuilder() {
    return AwsKmsConfigurationInfo.builder().roleArn("arn:aws:iam::123456789012:role/polaris-test");
  }
}
