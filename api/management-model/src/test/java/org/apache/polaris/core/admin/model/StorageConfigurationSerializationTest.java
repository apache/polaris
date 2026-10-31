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
package org.apache.polaris.core.admin.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public class StorageConfigurationSerializationTest {

  private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/finance";

  private ObjectMapper mapper;

  @BeforeEach
  public void setUp() {
    mapper = JsonMapper.shared();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("storageConfigurationTestCases")
  public void testStorageConfigurationSerialization(
      String description, StorageConfiguration storageConfiguration) {
    String json = mapper.writeValueAsString(storageConfiguration);
    StorageConfiguration deserialized = mapper.readValue(json, StorageConfiguration.class);
    assertThat(deserialized).usingRecursiveComparison().isEqualTo(storageConfiguration);
  }

  @Test
  public void testJsonFormat() {
    StorageConfiguration storageConfiguration =
        StorageConfiguration.builder(
                "finance",
                FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
                    .setAllowedLocations(List.of("file:///tmp/finance/"))
                    .build())
            .setBaseLocation("file:///tmp/finance/lake/")
            .build();

    String json =
        mapper.writeValueAsString(new CreateStorageConfigurationRequest(storageConfiguration));

    assertThat(json)
        .isEqualTo(
            "{\"storageConfiguration\":{"
                + "\"name\":\"finance\","
                + "\"storageConfigInfo\":{"
                + "\"storageType\":\"FILE\","
                + "\"allowedLocations\":[\"file:///tmp/finance/\"]"
                + "},"
                + "\"baseLocation\":\"file:///tmp/finance/lake/\""
                + "}}");
  }

  @Test
  public void testListJsonFormat() {
    StorageConfiguration storageConfiguration =
        new StorageConfiguration(
            "finance",
            FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
                .setAllowedLocations(List.of("file:///tmp/finance/"))
                .build());

    assertThat(mapper.writeValueAsString(new StorageConfigurations(List.of())))
        .isEqualTo("{\"storageConfigurations\":[]}");
    assertThat(
            mapper.writeValueAsString(
                new StorageConfigurations(List.of(storageConfiguration), "token-1")))
        .isEqualTo(
            "{\"storageConfigurations\":[{"
                + "\"name\":\"finance\","
                + "\"storageConfigInfo\":{"
                + "\"storageType\":\"FILE\","
                + "\"allowedLocations\":[\"file:///tmp/finance/\"]"
                + "}}],"
                + "\"nextPageToken\":\"token-1\"}");
  }

  private static Stream<Arguments> storageConfigurationTestCases() {
    return Stream.of(
        Arguments.of(
            "S3 with base location and create timestamp",
            StorageConfiguration.builder(
                    "finance",
                    AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                        .setRoleArn(TEST_ROLE_ARN)
                        .setAllowedLocations(List.of("s3://finance/"))
                        .build())
                .setBaseLocation("s3://finance/lake/")
                .setCreateTimestamp(1761868800000L)
                .build()),
        Arguments.of(
            "Azure without base location",
            new StorageConfiguration(
                "archive",
                AzureStorageConfigInfo.builder("tenant-id", StorageConfigInfo.StorageTypeEnum.AZURE)
                    .setAllowedLocations(List.of("abfss://archive@account.dfs.core.windows.net/"))
                    .build())),
        Arguments.of(
            "GCS",
            new StorageConfiguration(
                "ml",
                GcpStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.GCS)
                    .setAllowedLocations(List.of("gs://ml/"))
                    .build())));
  }
}
