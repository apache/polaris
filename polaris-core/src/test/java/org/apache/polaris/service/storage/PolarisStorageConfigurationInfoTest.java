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

package org.apache.polaris.service.storage;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Stream;
import org.apache.polaris.core.storage.FileStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class PolarisStorageConfigurationInfoTest {
  @InjectSoftAssertions protected SoftAssertions soft;

  private static ObjectMapper mapper;

  @BeforeAll
  public static void setup() {
    mapper = new ObjectMapper();
  }

  @ParameterizedTest
  @MethodSource
  public void reserialization(PolarisStorageConfigurationInfo configInfo, String serialized)
      throws Exception {
    var jsonStr = configInfo.serialize();

    var asJsonNode = mapper.readValue(jsonStr, JsonNode.class);
    var serializedJsonNode = mapper.readValue(serialized, JsonNode.class);
    soft.assertThat(asJsonNode).isEqualTo(serializedJsonNode);

    var deserialized = mapper.readValue(serialized, PolarisStorageConfigurationInfo.class);
    soft.assertThat(deserialized).isEqualTo(configInfo);

    var reserialized = mapper.readValue(jsonStr, PolarisStorageConfigurationInfo.class);
    soft.assertThat(reserialized).isEqualTo(configInfo);
  }

  static Stream<Arguments> reserialization() {
    return Stream.of(
        arguments(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations("s3://foo/bar", "s3://no/where")
                .roleARN("arn:aws:iam::123456789012:role/polaris-test")
                .region("no-where-1")
                .build(),
            "{\"@type\":\"AwsStorageConfigurationInfo\",\"storageType\":\"S3\",\"allowedLocations\":[\"s3://foo/bar\",\"s3://no/where\"],\"roleARN\":\"arn:aws:iam::123456789012:role/polaris-test\",\"region\":\"no-where-1\",\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\"}"),
        arguments(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations("s3://foo/bar", "s3://no/where")
                .region("no-where-1")
                .roleARN("arn:aws:iam::123456789012:role/polaris-test")
                .externalId("external-id")
                .build(),
            "{\"@type\":\"AwsStorageConfigurationInfo\",\"storageType\":\"S3\",\"allowedLocations\":[\"s3://foo/bar\",\"s3://no/where\"],\"roleARN\":\"arn:aws:iam::123456789012:role/polaris-test\",\"externalId\":\"external-id\",\"region\":\"no-where-1\",\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\"}"),
        arguments(
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations("s3://foo/bar", "s3://no/where")
                .region("no-where-1")
                .roleARN("arn:aws:iam::123456789012:role/polaris-test")
                .externalId("external-id")
                .endpoint("http://127.9.9.9/")
                .stsEndpoint("http://127.9.9.9/sts/")
                .endpointInternal("http://127.8.8.8/internal/")
                .pathStyleAccess(true)
                .build(),
            "{\"@type\":\"AwsStorageConfigurationInfo\",\"storageType\":\"S3\",\"allowedLocations\":[\"s3://foo/bar\",\"s3://no/where\"],\"roleARN\":\"arn:aws:iam::123456789012:role/polaris-test\",\"externalId\":\"external-id\",\"region\":\"no-where-1\",\"endpoint\":\"http://127.9.9.9/\",\"stsEndpoint\":\"http://127.9.9.9/sts/\",\"endpointInternal\":\"http://127.8.8.8/internal/\",\"pathStyleAccess\":true,\"fileIoImplClassName\":\"org.apache.iceberg.aws.s3.S3FileIO\"}"),
        //
        arguments(
            GcpStorageConfigurationInfo.builder()
                .addAllowedLocations("gs://foo/bar", "gs://meep/moo")
                .build(),
            "{\"@type\":\"GcpStorageConfigurationInfo\",\"allowedLocations\":[\"gs://foo/bar\",\"gs://meep/moo\"],\"storageType\":\"GCS\",\"fileIoImplClassName\":\"org.apache.iceberg.gcp.gcs.GCSFileIO\"}"),
        //
        arguments(
            AzureStorageConfigurationInfo.builder()
                .addAllowedLocations("abfs://foo@bar.baz/", "abfss://boo@meep.buzz/")
                .tenantId("tenant-id")
                .build(),
            "{\"@type\":\"AzureStorageConfigurationInfo\",\"allowedLocations\":[\"abfs://foo@bar.baz/\",\"abfss://boo@meep.buzz/\"],\"tenantId\":\"tenant-id\",\"storageType\":\"AZURE\",\"fileIoImplClassName\":\"org.apache.iceberg.azure.adlsv2.ADLSFileIO\"}"),
        //
        arguments(
            FileStorageConfigurationInfo.builder()
                .addAllowedLocations("file:///tmp/bar", "file:///meep/moo")
                .build(),
            "{\"@type\":\"FileStorageConfigurationInfo\",\"allowedLocations\":[\"file:///tmp/bar\",\"file:///meep/moo\"],\"storageType\":\"FILE\",\"fileIoImplClassName\":\"org.apache.iceberg.hadoop.HadoopFileIO\"}"));
  }
}
