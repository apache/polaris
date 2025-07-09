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
package org.apache.polaris.core.connection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectionConfigInfoDpoTest {
  private static final PolarisDiagnostics polarisDiagnostics = new PolarisDefaultDiagServiceImpl();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  }

  @Test
  void testOAuthClientCredentialsParameters() throws JsonProcessingException {
    // Test deserialization and reserialization of the persistence JSON.
    String json =
        ""
            + "{"
            + "  \"connectionTypeCode\": 1,"
            + "  \"uri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog\","
            + "  \"remoteCatalogName\": \"my-catalog\","
            + "  \"authenticationParameters\": {"
            + "    \"authenticationTypeCode\": 1,"
            + "    \"tokenUri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens\","
            + "    \"clientId\": \"client-id\","
            + "    \"clientSecretReference\": {"
            + "      \"urn\": \"urn:polaris-secret:secretmanager-impl:keystore-id-12345\","
            + "      \"referencePayload\": {"
            + "        \"hash\": \"a1b2c3\","
            + "        \"encryption-key\": \"z0y9x8\""
            + "      }"
            + "    },"
            + "    \"scopes\": [\"PRINCIPAL_ROLE:ALL\"]"
            + "  }"
            + "}";
    ConnectionConfigInfoDpo connectionConfigInfoDpo =
        ConnectionConfigInfoDpo.deserialize(polarisDiagnostics, json);
    Assertions.assertNotNull(connectionConfigInfoDpo);
    JsonNode tree1 = objectMapper.readTree(json);
    JsonNode tree2 = objectMapper.readTree(connectionConfigInfoDpo.serialize());
    Assertions.assertEquals(tree1, tree2);

    // Test conversion into API model JSON.
    ConnectionConfigInfo connectionConfigInfoApiModel =
        connectionConfigInfoDpo.asConnectionConfigInfoModel();
    String expectedApiModelJson =
        ""
            + "{"
            + "  \"connectionType\": \"ICEBERG_REST\","
            + "  \"uri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog\","
            + "  \"remoteCatalogName\": \"my-catalog\","
            + "  \"authenticationParameters\": {"
            + "    \"authenticationType\": \"OAUTH\","
            + "    \"tokenUri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens\","
            + "    \"clientId\": \"client-id\","
            + "    \"scopes\": [\"PRINCIPAL_ROLE:ALL\"]"
            + "  }"
            + "}";
    Assertions.assertEquals(
        objectMapper.readValue(expectedApiModelJson, ConnectionConfigInfo.class),
        connectionConfigInfoApiModel);
  }

  @Test
  void testBearerAuthenticationParameters() throws JsonProcessingException {
    // Test deserialization and reserialization of the persistence JSON.
    String json =
        ""
            + "{"
            + "  \"connectionTypeCode\": 1,"
            + "  \"uri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog\","
            + "  \"remoteCatalogName\": \"my-catalog\","
            + "  \"authenticationParameters\": {"
            + "    \"authenticationTypeCode\": 2,"
            + "    \"bearerTokenReference\": {"
            + "      \"urn\": \"urn:polaris-secret:secretmanager-impl:keystore-id-12345\","
            + "      \"referencePayload\": {"
            + "        \"hash\": \"a1b2c3\","
            + "        \"encryption-key\": \"z0y9x8\""
            + "      }"
            + "    }"
            + "  }"
            + "}";
    ConnectionConfigInfoDpo connectionConfigInfoDpo =
        ConnectionConfigInfoDpo.deserialize(polarisDiagnostics, json);
    Assertions.assertNotNull(connectionConfigInfoDpo);
    JsonNode tree1 = objectMapper.readTree(json);
    JsonNode tree2 = objectMapper.readTree(connectionConfigInfoDpo.serialize());
    Assertions.assertEquals(tree1, tree2);

    // Test conversion into API model JSON.
    ConnectionConfigInfo connectionConfigInfoApiModel =
        connectionConfigInfoDpo.asConnectionConfigInfoModel();
    String expectedApiModelJson =
        ""
            + "{"
            + "  \"connectionType\": \"ICEBERG_REST\","
            + "  \"uri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog\","
            + "  \"remoteCatalogName\": \"my-catalog\","
            + "  \"authenticationParameters\": {"
            + "    \"authenticationType\": \"BEARER\""
            + "  }"
            + "}";
    Assertions.assertEquals(
        objectMapper.readValue(expectedApiModelJson, ConnectionConfigInfo.class),
        connectionConfigInfoApiModel);
  }

  @Test
  void testImplicitAuthenticationParameters() throws JsonProcessingException {
    // Test deserialization and reserialization of the persistence JSON.
    String json =
        ""
            + "{"
            + "  \"connectionTypeCode\": 2,"
            + "  \"uri\": \"file:///hadoop-catalog/warehouse\","
            + "  \"warehouse\": \"hadoop-catalog\","
            + "  \"authenticationParameters\": {"
            + "    \"authenticationTypeCode\": 3"
            + "  }"
            + "}";
    ConnectionConfigInfoDpo connectionConfigInfoDpo =
        ConnectionConfigInfoDpo.deserialize(polarisDiagnostics, json);
    Assertions.assertNotNull(connectionConfigInfoDpo);
    JsonNode tree1 = objectMapper.readTree(json);
    JsonNode tree2 = objectMapper.readTree(connectionConfigInfoDpo.serialize());
    Assertions.assertEquals(tree1, tree2);

    // Test conversion into API model JSON.
    ConnectionConfigInfo connectionConfigInfoApiModel =
        connectionConfigInfoDpo.asConnectionConfigInfoModel();
    String expectedApiModelJson =
        ""
            + "{"
            + "  \"connectionType\": \"HADOOP\","
            + "  \"uri\": \"file:///hadoop-catalog/warehouse\","
            + "  \"warehouse\": \"hadoop-catalog\","
            + "  \"authenticationParameters\": {"
            + "    \"authenticationType\": \"IMPLICIT\""
            + "  }"
            + "}";
    Assertions.assertEquals(
        objectMapper.readValue(expectedApiModelJson, ConnectionConfigInfo.class),
        connectionConfigInfoApiModel);
  }
}
