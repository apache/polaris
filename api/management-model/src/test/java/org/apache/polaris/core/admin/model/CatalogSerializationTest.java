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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CatalogSerializationTest {

  private ObjectMapper mapper;
  private static final String TEST_LOCATION = "s3://test/";
  private static final String TEST_CATALOG_NAME = "test-catalog";
  private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";

  @BeforeEach
  public void setUp() {
    mapper = new ObjectMapper();
    // Mapper to use the concrete Catalog class
    mapper.addMixIn(PolarisCatalog.class, Catalog.class);
  }

  private Catalog verifyRoundTrip(Catalog original) throws JsonProcessingException {
    String json = mapper.writeValueAsString(original);
    int typeFieldCount = json.split("\"type\"").length - 1;
    assertThat(typeFieldCount)
        .as("type field should appear exactly once for %s catalog", original.getType())
        .isEqualTo(1);

    Catalog deserialized = mapper.readValue(json, Catalog.class);

    // Compare entire object recursively in one assertion
    assertThat(deserialized).isEqualToComparingFieldByFieldRecursively(original);

    return deserialized;
  }

  @ParameterizedTest
  @MethodSource("catalogTestCases")
  public void testCatalogSerialization(TestCase testCase) throws JsonProcessingException {
    verifyRoundTrip(testCase.getCatalog());
  }

  private static Stream<TestCase> catalogTestCases() {
    return Stream.of(
        new TestCase("Basic catalog")
            .withCatalog(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    TEST_CATALOG_NAME,
                    new CatalogProperties(TEST_LOCATION),
                    new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3))),
        new TestCase("Null fields")
            .withCatalog(new Catalog(Catalog.TypeEnum.INTERNAL, null, null, null)),
        new TestCase("Long name")
            .withCatalog(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "a".repeat(1000),
                    new CatalogProperties(TEST_LOCATION),
                    null)),
        new TestCase("Unicode characters")
            .withCatalog(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL, "测试目录", new CatalogProperties(TEST_LOCATION), null)),
        new TestCase("Empty strings")
            .withCatalog(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "",
                    new CatalogProperties(""),
                    new AwsStorageConfigInfo("", StorageConfigInfo.StorageTypeEnum.S3))),
        new TestCase("Special characters")
            .withCatalog(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "test\"catalog",
                    new CatalogProperties(TEST_LOCATION),
                    new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3))),
        new TestCase("Whitespace")
            .withCatalog(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "  test  catalog  ",
                    new CatalogProperties("  " + TEST_LOCATION + "  "),
                    null)));
  }

  private static class TestCase {
    private final String description;
    private Catalog catalog;

    TestCase(String description) {
      this.description = description;
    }

    TestCase withCatalog(Catalog catalog) {
      this.catalog = catalog;
      return this;
    }

    Catalog getCatalog() {
      return catalog;
    }

    @Override
    public String toString() {
      return description;
    }
  }
}
