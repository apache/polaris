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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CatalogSerializationTest {

  private ObjectMapper mapper;
  private static final String TEST_LOCATION = "s3://test/";
  private static final String TEST_CATALOG_NAME = "test-catalog";
  private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String KMS_KEY = "arn:aws:kms:us-east-1:012345678901:key/allowed-key-1";

  @BeforeEach
  public void setUp() {
    mapper = JsonMapper.builder().build();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("catalogTestCases")
  public void testCatalogSerialization(String description, Catalog catalog)
      throws JsonProcessingException {
    String json = mapper.writeValueAsString(catalog);
    Catalog deserialized = mapper.readValue(json, Catalog.class);
    assertThat(deserialized).usingRecursiveComparison().isEqualTo(catalog);
  }

  @Test
  public void testJsonFormat() throws JsonProcessingException {
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(TEST_CATALOG_NAME)
            .setProperties(new CatalogProperties(TEST_LOCATION))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                    .setRoleArn(TEST_ROLE_ARN)
                    .build())
            .build();

    String json = mapper.writeValueAsString(catalog);

    assertThat(json)
        .isEqualTo(
            "{\"type\":\"INTERNAL\","
                + "\"name\":\"test-catalog\","
                + "\"properties\":{\"default-base-location\":\"s3://test/\"},"
                + "\"storageConfigInfo\":{"
                + "\"roleArn\":\"arn:aws:iam::123456789012:role/test-role\","
                + "\"allowedKmsKeys\":[],"
                + "\"pathStyleAccess\":false,"
                + "\"storageType\":\"S3\","
                + "\"allowedLocations\":[]"
                + "}}");
  }

  @Test
  public void testJsonFormatWithKmsProperties() throws JsonProcessingException {
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(TEST_CATALOG_NAME)
            .setProperties(new CatalogProperties(TEST_LOCATION))
            .setStorageConfigInfo(
                AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                    .setRoleArn(TEST_ROLE_ARN)
                    .setCurrentKmsKey(KMS_KEY)
                    .build())
            .build();

    String json = mapper.writeValueAsString(catalog);

    assertThat(json)
        .isEqualTo(
            "{\"type\":\"INTERNAL\","
                + "\"name\":\"test-catalog\","
                + "\"properties\":{\"default-base-location\":\"s3://test/\"},"
                + "\"storageConfigInfo\":{"
                + "\"roleArn\":\"arn:aws:iam::123456789012:role/test-role\","
                + "\"currentKmsKey\":\"arn:aws:kms:us-east-1:012345678901:key/allowed-key-1\","
                + "\"allowedKmsKeys\":[],"
                + "\"pathStyleAccess\":false,"
                + "\"storageType\":\"S3\","
                + "\"allowedLocations\":[]"
                + "}}");
  }

  @Test
  public void testExternalCatalogWithoutStorageConfig() throws JsonProcessingException {
    Catalog catalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(TEST_CATALOG_NAME)
            .setProperties(new CatalogProperties(TEST_LOCATION))
            .build();

    String json = mapper.writeValueAsString(catalog);
    Catalog deserialized = mapper.readValue(json, Catalog.class);

    assertThat(deserialized).usingRecursiveComparison().isEqualTo(catalog);
    assertThat(json).doesNotContain("storageConfigInfo");
  }

  private static Stream<Arguments> catalogTestCases() {
    Stream<Arguments> basicCases =
        Stream.of(
            Arguments.of(
                "Basic catalog",
                PolarisCatalog.builder()
                    .setType(Catalog.TypeEnum.INTERNAL)
                    .setName(TEST_CATALOG_NAME)
                    .setProperties(new CatalogProperties(TEST_LOCATION))
                    .setStorageConfigInfo(
                        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                            .setRoleArn(TEST_ROLE_ARN)
                            .build())
                    .build()),
            Arguments.of(
                "Null fields",
                PolarisCatalog.builder()
                    .setType(Catalog.TypeEnum.INTERNAL)
                    .setName(null)
                    .setProperties(null)
                    .build()),
            Arguments.of(
                "Long name",
                PolarisCatalog.builder()
                    .setType(Catalog.TypeEnum.INTERNAL)
                    .setName("a".repeat(1000))
                    .setProperties(new CatalogProperties(TEST_LOCATION))
                    .build()),
            Arguments.of(
                "Unicode characters",
                PolarisCatalog.builder()
                    .setType(Catalog.TypeEnum.INTERNAL)
                    .setName("测试目录")
                    .setProperties(new CatalogProperties(TEST_LOCATION))
                    .build()),
            Arguments.of(
                "Empty strings",
                PolarisCatalog.builder()
                    .setType(Catalog.TypeEnum.INTERNAL)
                    .setName("")
                    .setProperties(new CatalogProperties(""))
                    .setStorageConfigInfo(
                        new AwsStorageConfigInfo(StorageConfigInfo.StorageTypeEnum.S3))
                    .build()),
            Arguments.of(
                "Special characters",
                PolarisCatalog.builder()
                    .setType(Catalog.TypeEnum.INTERNAL)
                    .setName("test\"catalog")
                    .setProperties(new CatalogProperties(TEST_LOCATION))
                    .setStorageConfigInfo(
                        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                            .setRoleArn(TEST_ROLE_ARN)
                            .build())
                    .build()),
            Arguments.of(
                "Whitespace",
                PolarisCatalog.builder()
                    .setType(Catalog.TypeEnum.INTERNAL)
                    .setName("  test  catalog  ")
                    .setProperties(new CatalogProperties("  " + TEST_LOCATION + "  "))
                    .build()),
            Arguments.of(
                "External catalog without storage config",
                ExternalCatalog.builder()
                    .setType(Catalog.TypeEnum.EXTERNAL)
                    .setName(TEST_CATALOG_NAME)
                    .setProperties(new CatalogProperties(TEST_LOCATION))
                    .build()));

    Stream<Arguments> arnCases =
        Stream.of(
                "arn:aws:iam::123456789012:role/test-role",
                "arn:aws:iam::123456789012:role/service-role/test-role",
                "arn:aws:iam::123456789012:role/path/to/role")
            .map(
                arn ->
                    Arguments.of(
                        "ARN: " + arn,
                        PolarisCatalog.builder()
                            .setType(Catalog.TypeEnum.INTERNAL)
                            .setName(TEST_CATALOG_NAME)
                            .setProperties(new CatalogProperties(TEST_LOCATION))
                            .setStorageConfigInfo(
                                AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
                                    .setRoleArn(arn)
                                    .build())
                            .build()));

    return Stream.concat(basicCases, arnCases);
  }
}
