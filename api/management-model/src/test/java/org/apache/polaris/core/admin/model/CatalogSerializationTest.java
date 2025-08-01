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

  @BeforeEach
  public void setUp() {
    mapper = new ObjectMapper();
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
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            TEST_CATALOG_NAME,
            new CatalogProperties(TEST_LOCATION),
            new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3));

    String json = mapper.writeValueAsString(catalog);

    assertThat(json)
        .isEqualTo(
            "{\"type\":\"INTERNAL\","
                + "\"name\":\"test-catalog\","
                + "\"properties\":{\"default-base-location\":\"s3://test/\"},"
                + "\"storageConfigInfo\":{"
                + "\"roleArn\":\"arn:aws:iam::123456789012:role/test-role\","
                + "\"pathStyleAccess\":false,"
                + "\"storageType\":\"S3\","
                + "\"allowedLocations\":[]"
                + "}}");
  }

  private static Stream<Arguments> catalogTestCases() {
    Stream<Arguments> basicCases =
        Stream.of(
            Arguments.of(
                "Basic catalog",
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    TEST_CATALOG_NAME,
                    new CatalogProperties(TEST_LOCATION),
                    new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3))),
            Arguments.of("Null fields", new Catalog(Catalog.TypeEnum.INTERNAL, null, null, null)),
            Arguments.of(
                "Long name",
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "a".repeat(1000),
                    new CatalogProperties(TEST_LOCATION),
                    null)),
            Arguments.of(
                "Unicode characters",
                new Catalog(
                    Catalog.TypeEnum.INTERNAL, "测试目录", new CatalogProperties(TEST_LOCATION), null)),
            Arguments.of(
                "Empty strings",
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "",
                    new CatalogProperties(""),
                    new AwsStorageConfigInfo("", StorageConfigInfo.StorageTypeEnum.S3))),
            Arguments.of(
                "Special characters",
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "test\"catalog",
                    new CatalogProperties(TEST_LOCATION),
                    new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3))),
            Arguments.of(
                "Whitespace",
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "  test  catalog  ",
                    new CatalogProperties("  " + TEST_LOCATION + "  "),
                    null)));

    Stream<Arguments> arnCases =
        Stream.of(
                "arn:aws:iam::123456789012:role/test-role",
                "arn:aws:iam::123456789012:role/service-role/test-role",
                "arn:aws:iam::123456789012:role/path/to/role")
            .map(
                arn ->
                    Arguments.of(
                        "ARN: " + arn,
                        new Catalog(
                            Catalog.TypeEnum.INTERNAL,
                            TEST_CATALOG_NAME,
                            new CatalogProperties(TEST_LOCATION),
                            new AwsStorageConfigInfo(arn, StorageConfigInfo.StorageTypeEnum.S3))));

    return Stream.concat(basicCases, arnCases);
  }
}
