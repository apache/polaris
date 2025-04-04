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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.polaris.core.PolarisDiagnostics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PolarisConnectionConfigurationInfoTest {
  PolarisDiagnostics polarisDiagnostics = Mockito.mock(PolarisDiagnostics.class);
  ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void testOAuthClientCredentialsParameters() throws JsonProcessingException {
    String json =
        ""
            + "{"
            + "  \"connectionType\": \"ICEBERG_REST\","
            + "  \"uri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog\","
            + "  \"remoteCatalogName\": \"my-catalog\","
            + "  \"restAuthentication\": {"
            + "    \"restAuthenticationType\": \"OAUTH\","
            + "    \"tokenUri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens\","
            + "    \"clientId\": \"client-id\","
            + "    \"clientSecret\": \"client-secret\","
            + "    \"scopes\": [\"PRINCIPAL_ROLE:ALL\"]"
            + "  }"
            + "}";
    PolarisConnectionConfigurationInfo connectionConfigurationInfo =
        PolarisConnectionConfigurationInfo.deserialize(polarisDiagnostics, json);
    Assertions.assertNotNull(connectionConfigurationInfo);
    System.out.println(connectionConfigurationInfo.serialize());
    JsonNode tree1 = objectMapper.readTree(json);
    JsonNode tree2 = objectMapper.readTree(connectionConfigurationInfo.serialize());
    Assertions.assertEquals(tree1, tree2);
  }

  @Test
  void testBearerAuthenticationParameters() throws JsonProcessingException {
    String json =
        ""
            + "{"
            + "  \"connectionType\": \"ICEBERG_REST\","
            + "  \"uri\": \"https://myorg-my_account.snowflakecomputing.com/polaris/api/catalog\","
            + "  \"remoteCatalogName\": \"my-catalog\","
            + "  \"restAuthentication\": {"
            + "    \"restAuthenticationType\": \"BEARER\","
            + "    \"bearerToken\": \"bearer-token\""
            + "  }"
            + "}";
    PolarisConnectionConfigurationInfo connectionConfigurationInfo =
        PolarisConnectionConfigurationInfo.deserialize(polarisDiagnostics, json);
    Assertions.assertNotNull(connectionConfigurationInfo);
    System.out.println(connectionConfigurationInfo.serialize());
    JsonNode tree1 = objectMapper.readTree(json);
    JsonNode tree2 = objectMapper.readTree(connectionConfigurationInfo.serialize());
    Assertions.assertEquals(tree1, tree2);
  }
}
