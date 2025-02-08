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
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
  }

  /**
   * Helper method to verify round-trip serialization/deserialization of Catalog
   * objects. Ensures all fields are preserved correctly through the process.
   *
   * @param original The catalog object to test
   * @return The deserialized catalog for additional assertions if needed
   */
  private Catalog verifyRoundTrip(Catalog original) throws JsonProcessingException {
    // Perform serialization and deserialization
    String json = mapper.writeValueAsString(original);
    Catalog deserialized = mapper.readValue(json, Catalog.class);

    // Compare the content instead of direct object equality
    assertThat(deserialized.getType()).isEqualTo(original.getType());
    assertThat(deserialized.getName()).isEqualTo(original.getName());
    assertThat(deserialized.getProperties())
        .usingRecursiveComparison()
        .isEqualTo(original.getProperties());
    assertThat(deserialized.getStorageConfigInfo())
        .usingRecursiveComparison()
        .isEqualTo(original.getStorageConfigInfo());
    assertThat(deserialized.getCreateTimestamp()).isEqualTo(original.getCreateTimestamp());
    assertThat(deserialized.getLastUpdateTimestamp()).isEqualTo(original.getLastUpdateTimestamp());
    assertThat(deserialized.getEntityVersion()).isEqualTo(original.getEntityVersion());

    return deserialized;
  }

  @ParameterizedTest
  @MethodSource("catalogTestCases")
  public void testCatalogSerialization(TestCase testCase) throws JsonProcessingException {
    verifyRoundTrip(testCase.catalog);
  }

  private static Stream<TestCase> catalogTestCases() {
    Stream<TestCase> basicCases = Stream.of(
        new TestCase("Basic catalog")
            .withCatalog(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    TEST_CATALOG_NAME,
                    new CatalogProperties(TEST_LOCATION),
                    new AwsStorageConfigInfo(
                        TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3))),
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
                    Catalog.TypeEnum.INTERNAL,
                    "测试目录",
                    new CatalogProperties(TEST_LOCATION),
                    null)),
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
                    new AwsStorageConfigInfo(
                        TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3))),
        new TestCase("Whitespace")
            .withCatalog(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "  test  catalog  ",
                    new CatalogProperties("  " + TEST_LOCATION + "  "),
                    null)));

    Stream<TestCase> arnCases = Stream.of(
        "arn:aws:iam::123456789012:role/test-role",
        "arn:aws:iam::123456789012:role/service-role/test-role",
        "arn:aws:iam::123456789012:role/path/to/role")
        .map(
            arn -> new TestCase("ARN: " + arn)
                .withCatalog(
                    new Catalog(
                        Catalog.TypeEnum.INTERNAL,
                        TEST_CATALOG_NAME,
                        new CatalogProperties(TEST_LOCATION),
                        new AwsStorageConfigInfo(
                            arn, StorageConfigInfo.StorageTypeEnum.S3))));

    return Stream.concat(basicCases, arnCases);
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

    @Override
    public String toString() {
      return description;
    }
  }

  @Test
  public void testMalformedJson() {
    String json = "{" + "\"type\": \"INTERNAL\"," + "\"name\": \"test-catalog\"," + "\"properties\": {" + "}";
    assertThrows(JsonProcessingException.class, () -> mapper.readValue(json, Catalog.class));
  }
}
