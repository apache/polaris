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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// Tests for serialization and deserialization of Catalog objects
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

  // Tests basic catalog object serialization to JSON and verifies type field
  // handling
  @Test
  public void testCatalogSerialization() throws JsonProcessingException {
    CatalogProperties properties = new CatalogProperties(TEST_LOCATION);

    StorageConfigInfo storageConfig = new StorageConfigInfo(StorageConfigInfo.StorageTypeEnum.S3,
        Collections.emptyList());

    Catalog catalog = new Catalog(Catalog.TypeEnum.INTERNAL, TEST_CATALOG_NAME, properties, storageConfig);

    String json = mapper.writeValueAsString(catalog);

    // Assert exact JSON string
    String expectedJson = "{" +
        "\"type\":\"INTERNAL\"," +
        "\"name\":\"test-catalog\"," +
        "\"properties\":{" +
        "\"default-base-location\":\"s3://test/\"" +
        "}," +
        "\"createTimestamp\":null," +
        "\"lastUpdateTimestamp\":null," +
        "\"entityVersion\":null," +
        "\"storageConfigInfo\":{" +
        "\"storageType\":\"S3\"," +
        "\"allowedLocations\":[]" +
        "}" +
        "}";

    assertEquals(expectedJson, json);
  }

  // Tests deserialization of JSON string into Catalog object and validates field
  // values
  @Test
  public void testCatalogDeserialization() throws JsonProcessingException {
    String json = "{"
        + "\"type\": \"INTERNAL\","
        + "\"name\": \"test-catalog\","
        + "\"properties\": {"
        + "    \"default-base-location\": \"s3://test/\""
        + "},"
        + "\"storageConfigInfo\": {"
        + "    \"storageType\": \"S3\","
        + "    \"roleArn\": \"arn:aws:iam::123456789012:role/test-role\","
        + "    \"allowedLocations\": []"
        + "}"
        + "}";

    Catalog catalog = mapper.readValue(json, Catalog.class);

    assertEquals(Catalog.TypeEnum.INTERNAL, catalog.getType());
    assertEquals(TEST_CATALOG_NAME, catalog.getName());
    assertEquals(TEST_LOCATION, catalog.getProperties().getDefaultBaseLocation());
  }

  // Tests handling of null fields during serialization and deserialization
  @Test
  public void testCatalogWithNullFields() throws JsonProcessingException {
    // Create catalog with null fields except type (required)
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL, // type cannot be null for polymorphic deserialization
        null, // null name
        null, // null properties
        null // null storage config
    );

    // Test serialization
    String json = mapper.writeValueAsString(catalog);

    // Test deserialization
    Catalog deserializedCatalog = mapper.readValue(json, Catalog.class);

    // Verify fields
    assertEquals(Catalog.TypeEnum.INTERNAL, deserializedCatalog.getType());
    assertNull(deserializedCatalog.getName());
    assertNull(deserializedCatalog.getProperties());
    assertNull(deserializedCatalog.getStorageConfigInfo());
  }

  // Tests error handling for invalid JSON input during deserialization
  @Test
  public void testInvalidJsonDeserialization() {
    String invalidJson = "{ invalid json }";
    assertThrows(JsonProcessingException.class, () -> mapper.readValue(invalidJson, Catalog.class));
  }

  // Tests handling of empty string values in catalog fields
  @Test
  public void testCatalogWithEmptyFields() throws JsonProcessingException {
    String json = "{"
        + "\"type\": \"INTERNAL\","
        + "\"name\": \"\","
        + "\"properties\": {"
        + "    \"default-base-location\": \"\""
        + "},"
        + "\"storageConfigInfo\": {"
        + "    \"storageType\": \"S3\","
        + "    \"roleArn\": \"arn:aws:iam::123456789012:role/empty\","
        + "    \"allowedLocations\": []"
        + "}"
        + "}";

    Catalog catalog = mapper.readValue(json, Catalog.class);
    assertEquals("", catalog.getName());
    assertEquals("", catalog.getProperties().getDefaultBaseLocation());
  }

  // Tests handling of special characters in catalog name
  @Test
  public void testSpecialCharacters() throws JsonProcessingException {
    StorageConfigInfo storageConfig = new AwsStorageConfigInfo(TEST_ROLE_ARN, StorageConfigInfo.StorageTypeEnum.S3);

    String specialName = "test\"catalog";
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL,
        specialName,
        new CatalogProperties(TEST_LOCATION),
        storageConfig);

    String json = mapper.writeValueAsString(catalog);
    JsonNode node = mapper.readTree(json);
    assertEquals(specialName, node.get("name").asText());
  }

  // Tests serialization and deserialization of empty string values
  @Test
  public void testCatalogWithEmptyStrings() throws JsonProcessingException {
    String json = "{"
        + "\"type\": \"INTERNAL\","
        + "\"name\": \"\","
        + "\"properties\": {"
        + "    \"default-base-location\": \"\""
        + "},"
        + "\"storageConfigInfo\": {"
        + "    \"storageType\": \"S3\","
        + "    \"roleArn\": \"\","
        + "    \"allowedLocations\": []"
        + "}"
        + "}";

    Catalog catalog = mapper.readValue(json, Catalog.class);
    String serialized = mapper.writeValueAsString(catalog);
    JsonNode node = mapper.readTree(serialized);

    assertEquals("", node.get("name").asText());
    assertEquals("", node.at("/properties/default-base-location").asText());
    assertEquals("", node.at("/storageConfigInfo/roleArn").asText());
  }

  // Tests error handling for invalid enum values during deserialization
  @Test
  public void testInvalidEnumValue() {
    String json = "{" + "\"type\": \"INVALID_TYPE\"," + "\"name\": \"test-catalog\"" + "}";

    assertThrows(JsonMappingException.class, () -> mapper.readValue(json, Catalog.class));
  }

  // Tests error handling for malformed JSON structure
  @Test
  public void testMalformedJson() {
    String json = "{" + "\"type\": \"INTERNAL\"," + "\"name\": \"test-catalog\"," + "\"properties\": {" + "}";

    assertThrows(JsonProcessingException.class, () -> mapper.readValue(json, Catalog.class));
  }

  // Tests handling of very long catalog names
  @Test
  public void testLongCatalogName() throws JsonProcessingException {
    String longName = "a".repeat(1000);
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL, longName, new CatalogProperties(TEST_LOCATION), null);
    String json = mapper.writeValueAsString(catalog);
    Catalog deserialized = mapper.readValue(json, Catalog.class);
    assertEquals(longName, deserialized.getName());
  }

  // Tests handling of Unicode characters in catalog names
  @Test
  public void testUnicodeCharacters() throws JsonProcessingException {
    String unicodeName = "测试目录";
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL, unicodeName, new CatalogProperties(TEST_LOCATION), null);
    String json = mapper.writeValueAsString(catalog);
    Catalog deserialized = mapper.readValue(json, Catalog.class);
    assertEquals(unicodeName, deserialized.getName());
  }

  // Tests preservation of whitespace in field values
  @Test
  public void testWhitespaceHandling() throws JsonProcessingException {
    String nameWithSpaces = "  test  catalog  ";
    Catalog catalog = new Catalog(
        Catalog.TypeEnum.INTERNAL,
        nameWithSpaces,
        new CatalogProperties("  " + TEST_LOCATION + "  "),
        null);
    String json = mapper.writeValueAsString(catalog);
    Catalog deserialized = mapper.readValue(json, Catalog.class);
    assertEquals(nameWithSpaces, deserialized.getName());
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

      String json = mapper.writeValueAsString(catalog);
      Catalog deserialized = mapper.readValue(json, Catalog.class);
      assertEquals(arn, ((AwsStorageConfigInfo) deserialized.getStorageConfigInfo()).getRoleArn());
    }
  }

  private int countTypeFields(JsonNode node) {
    int count = 0;
    if (node.has("type")) {
      count++;
    }
    if (node.isObject()) {
      for (JsonNode child : node) {
        count += countTypeFields(child);
      }
    }
    return count;
  }
}
