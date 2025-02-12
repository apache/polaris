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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
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

  /**
   * Helper method to verify round-trip serialization/deserialization of Catalog objects. Ensures
   * all fields are preserved correctly through the process.
   *
   * @param original The catalog object to test
   * @return The deserialized catalog for additional assertions if needed
   */
  private Catalog verifyRoundTrip(Catalog original) throws JsonProcessingException {
    String json = mapper.writeValueAsString(original);

    // Verify type field appears exactly once
    int typeFieldCount = json.split("\"type\"").length - 1;
    assertThat(typeFieldCount)
        .as("type field should appear exactly once for %s catalog", original.getType())
        .isEqualTo(1);

    Catalog deserialized = mapper.readValue(json, Catalog.class);

    // Compare all fields recursively, ignoring actual types
    assertThat(deserialized).usingRecursiveComparison().isEqualTo(original);

    return deserialized;
  }

  @ParameterizedTest
  @MethodSource("catalogTestCases")
  public void testCatalogSerialization(Catalog catalog) throws JsonProcessingException {
    verifyRoundTrip(catalog);
  }

  private static Stream<Arguments> typeTestCases() {
    return Stream.of(arguments(Catalog.TypeEnum.INTERNAL), arguments(Catalog.TypeEnum.EXTERNAL));
  }

  private static Stream<Arguments> catalogTestCases() {
    Stream<Arguments> basicCases =
        Stream.of(
            arguments(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    TEST_CATALOG_NAME,
                    new CatalogProperties(TEST_LOCATION),
                    new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3))),
            arguments(new Catalog(Catalog.TypeEnum.INTERNAL, null, null, null)),
            arguments(
                new Catalog(
                    Catalog.TypeEnum.INTERNAL,
                    "a".repeat(1000),
                    new CatalogProperties(TEST_LOCATION),
                    null)));

    Stream<Arguments> arnCases =
        Stream.of(
                "arn:aws:iam::123456789012:role/test-role",
                "arn:aws:iam::123456789012:role/service-role/test-role",
                "arn:aws:iam::123456789012:role/path/to/role")
            .map(
                arn ->
                    arguments(
                        new Catalog(
                            Catalog.TypeEnum.INTERNAL,
                            TEST_CATALOG_NAME,
                            new CatalogProperties(TEST_LOCATION),
                            new AwsStorageConfigInfo(arn, StorageConfigInfo.StorageTypeEnum.S3))));

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
}
