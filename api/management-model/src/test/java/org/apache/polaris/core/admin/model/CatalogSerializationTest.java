package org.apache.polaris.core.admin.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Collections;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo.StorageTypeEnum;

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

    @Test
    public void testCatalogSerialization() throws JsonProcessingException {
        CatalogProperties properties = new CatalogProperties(TEST_LOCATION);

        StorageConfigInfo storageConfig = new StorageConfigInfo(
                StorageConfigInfo.StorageTypeEnum.S3,
                Collections.emptyList());

        Catalog catalog = new Catalog(
                Catalog.TypeEnum.INTERNAL,
                TEST_CATALOG_NAME,
                properties,
                storageConfig);

        String json = mapper.writeValueAsString(catalog);
        JsonNode jsonNode = mapper.readTree(json);

        assertEquals(1, countTypeFields(jsonNode));
        assertEquals("INTERNAL", jsonNode.get("type").asText());
    }

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

    @Test
    public void testInvalidJsonDeserialization() {
        String invalidJson = "{ invalid json }";
        assertThrows(JsonProcessingException.class, () -> mapper.readValue(invalidJson, Catalog.class));
    }

    @Test
    public void testCatalogWithEmptyFields() throws JsonProcessingException {
        StringBuilder json = new StringBuilder();
        json.append("{")
                .append("\"type\": \"INTERNAL\",")
                .append("\"name\": \"\",")
                .append("\"properties\": {")
                .append("    \"default-base-location\": \"\"")
                .append("},")
                .append("\"storageConfigInfo\": {")
                .append("    \"storageType\": \"S3\",")
                .append("    \"roleArn\": \"arn:aws:iam::123456789012:role/empty\",")
                .append("    \"allowedLocations\": []")
                .append("}")
                .append("}");

        Catalog catalog = mapper.readValue(json.toString(), Catalog.class);
        assertEquals("", catalog.getName());
        assertEquals("", catalog.getProperties().getDefaultBaseLocation());
    }

    @Test
    public void testSpecialCharacters() throws JsonProcessingException {
        StorageConfigInfo storageConfig = new AwsStorageConfigInfo(
                TEST_ROLE_ARN,
                StorageConfigInfo.StorageTypeEnum.S3);

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
