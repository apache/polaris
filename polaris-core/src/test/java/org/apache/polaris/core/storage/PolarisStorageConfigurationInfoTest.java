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
package org.apache.polaris.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.storage.aws.ImmutableAwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.ImmutableAzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.ImmutableGcpStorageConfigurationInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PolarisStorageConfigurationInfoTest {

  public static Stream<Arguments> serialize() {
    return Stream.of(
        Arguments.of(
            ImmutableAwsStorageConfigurationInfo.builder()
                .allowedLocations(
                    List.of(
                        "s3://bucket/path/to/warehouse",
                        "s3://bucket/anotherpath/to/warehouse",
                        "s3://bucket2/warehouse/"))
                .roleARN("arn:aws:iam::012345678901:role/jdoe")
                .externalId("externalId")
                .userARN("userARN")
                .build(),
            "AwsStorageConfigurationInfo"),
        Arguments.of(
            ImmutableAzureStorageConfigurationInfo.builder()
                .allowedLocations(
                    List.of(
                        "abfs://account@container.com/path/to/warehouse",
                        "abfs://account@container.com/anotherpath/to/warehouse",
                        "abfs://account2@container.com/warehouse/"))
                .consentUrl("https://login.microsoftonline.com/tenantId/oauth2/authorize")
                .multiTenantAppName("appName")
                .tenantId("tenantId")
                .build(),
            "AzureStorageConfigurationInfo"),
        Arguments.of(
            ImmutableGcpStorageConfigurationInfo.builder()
                .allowedLocations(
                    List.of(
                        "gs://bucket/path/to/warehouse",
                        "gs://bucket/anotherpath/to/warehouse",
                        "gs://bucket2/warehouse/"))
                .gcpServiceAccount("serviceAccount")
                .build(),
            "GcpStorageConfigurationInfo"),
        Arguments.of(
            ImmutableFileStorageConfigurationInfo.builder()
                .allowedLocations(
                    List.of(
                        "file:///path/to/warehouse",
                        "file:///anotherpath/to/warehouse",
                        "file:///warehouse/"))
                .build(),
            "FileStorageConfigurationInfo"));
  }

  @ParameterizedTest
  @MethodSource
  void serialize(PolarisStorageConfigurationInfo instance, String typeName) {
    String json = instance.serialize();
    assertThat(json).contains("\"@type\":\"" + typeName + "\"");
    PolarisStorageConfigurationInfo deserialized =
        PolarisStorageConfigurationInfo.deserialize(new PolarisDefaultDiagServiceImpl(), json);
    assertThat(deserialized).isEqualTo(instance);
  }
}
