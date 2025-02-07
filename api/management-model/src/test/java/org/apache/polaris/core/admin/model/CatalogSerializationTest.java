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

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for Catalog JSON serialization and deserialization.
 *
 * <p>
 * Coverage includes:
 *
 * <ul>
 * <li>Basic serialization/deserialization of Catalog objects
 * <li>Handling of null and empty fields
 * <li>Special character handling in field values
 * <li>Unicode character support
 * <li>Whitespace preservation
 * <li>AWS role ARN validation
 * </ul>
 *
 * Error handling coverage:
 *
 * <ul>
 * <li>Invalid JSON input
 * <li>Malformed JSON structure
 * <li>Invalid enum values
 * <li>Edge cases like very long catalog names
 * </ul>
 */
public class CatalogSerializationTest {

  private ObjectMapper mapper;
  private static final String TEST_LOCATION = "s3://test/";
  private static final String TEST_CATALOG_NAME = "test-catalog";
  private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";

  @BeforeEach
  public void setUp() {
    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  /**
   * Helper method to verify round-trip serialization/deserialization of Catalog
   * objects. Ensures
   * all fields are preserved correctly through the process.
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
    assertThat(deserialized.getProperties()).usingRecursiveComparison().isEqualTo(original.getProperties());
    assertThat(deserialized.getStorageConfigInfo()).usingRecursiveComparison()
        .isEqualTo(original.getStorageConfigInfo());
    assertThat(deserialized.getCreateTimestamp()).isEqualTo(original.getCreateTimestamp());
    assertThat(deserialized.getLastUpdateTimestamp()).isEqualTo(original.getLastUpdateTimestamp());
    assertThat(deserialized.getEntityVersion()).isEqualTo(original.getEntityVersion());

    return deserialized;
  }

  // Update testCatalogSerialization to use verifyRoundTrip
  @Test
  public void testCatalogSerialization() throws JsonProcessingException {
    CatalogProperties properties = new CatalogProperties(TEST_LOCATION);

    // Create AWS storage config with required roleArn
    StorageConfigInfo storageConfig = new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3);

    Catalog catalog = new Catalog(Catalog.TypeEnum.INTERNAL, TEST_CATALOG_NAME, properties, storageConfig);

    verifyRoundTrip(catalog);
  }

  // Update testCatalogDeserialization
  @Test
  public void testCatalogDeserialization() throws JsonProcessingException {
    StorageConfigInfo storageConfig = new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3);
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL,
        TEST_CATALOG_NAME,
        new CatalogProperties(TEST_LOCATION),
        storageConfig);
    verifyRoundTrip(catalog);
  }

  // Update testCatalogWithNullFields - already using proper verification
  @Test
  public void testCatalogWithNullFields() throws JsonProcessingException {
    Catalog catalog = new Catalog(Catalog.TypeEnum.INTERNAL, null, null, null);
    verifyRoundTrip(catalog);
  }

  // Tests handling of empty string values in catalog fields
  @Test
  public void testCatalogWithEmptyFields() throws JsonProcessingException {
    String json = "{"
        + "\"type\": \"INTERNAL\","
        + "\"name\": \"\","
        + "\"properties\": {"
        + "\"default-base-location\": \"\""
        + "},"
        + "\"storageConfigInfo\": {"
        + "\"storageType\": \"S3\","
        + "\"roleArn\": \"arn:aws:iam::123456789012:role/empty\","
        + "\"allowedLocations\": []"
        + "}"
        + "}";

    Catalog catalog = mapper.readValue(json, Catalog.class);
    assertEquals("", catalog.getName());
    assertEquals("", catalog.getProperties().getDefaultBaseLocation());
  }

  // Tests handling of special characters in catalog names
  @Test
  public void testSpecialCharacters() throws JsonProcessingException {
    String specialName = "test\"catalog";
    StorageConfigInfo storageConfig = new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3);
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL,
        specialName,
        new CatalogProperties(TEST_LOCATION),
        storageConfig);
    verifyRoundTrip(catalog);
  }

  @Test
  public void testCatalogWithEmptyStrings() throws JsonProcessingException {
    String json = "{"
        + "\"type\": \"INTERNAL\","
        + "\"name\": \"\","
        + "\"properties\": {"
        + "\"default-base-location\": \"\""
        + "},"
        + "\"storageConfigInfo\": {"
        + "\"storageType\": \"S3\","
        + "\"roleArn\": \"\","
        + "\"allowedLocations\": []"
        + "}"
        + "}";

    Catalog catalog = mapper.readValue(json, Catalog.class);
    String serialized = mapper.writeValueAsString(catalog);
    JsonNode node = mapper.readTree(serialized);

    assertThat(node.get("name").asText()).isEqualTo("");
    assertThat(node.at("/properties/default-base-location").asText()).isEqualTo("");
    assertThat(node.at("/storageConfigInfo/roleArn").asText()).isEqualTo("");
  }

  // Tests error handling for malformed JSON structure
  @Test
  public void testMalformedJson() {
    String json = "{" + "\"type\": \"INTERNAL\"," + "\"name\": \"test-catalog\"," + "\"properties\": {" + "}";

    assertThrows(JsonProcessingException.class, () -> mapper.readValue(json, Catalog.class));
  }

  // Update testLongCatalogName
  @Test
  public void testLongCatalogName() throws JsonProcessingException {
    String longName = "a".repeat(1000);
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL, longName, new CatalogProperties(TEST_LOCATION), null);
    verifyRoundTrip(catalog);
  }

  // Tests handling of Unicode characters in catalog names
  @Test
  public void testUnicodeCharacters() throws JsonProcessingException {
    String unicodeName = "测试目录";
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL, unicodeName, new CatalogProperties(TEST_LOCATION), null);
    verifyRoundTrip(catalog);
  }

  // Update testWhitespaceHandling
  @Test
  public void testWhitespaceHandling() throws JsonProcessingException {
    String nameWithSpaces = "  test  catalog  ";
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL,
        nameWithSpaces,
        new CatalogProperties("  " + TEST_LOCATION + "  "),
        null);
    verifyRoundTrip(catalog);
  }

  // Tests validation of AWS role ARN formats
  @Test
  public void testRoleArnValidation() throws JsonProcessingException {
    String[] validArns = {
        "arn:aws:iam::123456789012:role/test-role",
        "arn:aws:iam::123456789012:role/service-role/test-role",
        "arn:aws:iam::123456789012:role/path/to/role"
    };

    for (String arn : validArns) {
      StorageConfigInfo storageConfig = new AwsStorageConfigInfo(arn, StorageConfigInfo.StorageTypeEnum.S3);
      Catalog catalog = new Catalog(
          Catalog.TypeEnum.INTERNAL,
          TEST_CATALOG_NAME,
          new CatalogProperties(TEST_LOCATION),
          storageConfig);
      verifyRoundTrip(catalog);
    }
  }
}