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

package org.apache.polaris.core.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.apache.iceberg.expressions.Expression;
import org.apache.polaris.core.policy.content.AccessControlPolicyContent;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AccessControlPolicyContentTest {
  @Test
  @DisplayName("Should deserialize a full policy with all fields correctly")
  void testFromString_fullPolicy() {
    String jsonContent =
        "{\n"
            + "  \"principalRole\": \"ANALYST\",\n"
            + "  \"columnProjections\": [\"name\", \"location\"],\n"
            + "  \"rowFilters\": [\n"
            + "    {\n"
            + "      \"type\": \"eq\",\n"
            + "      \"term\": \"country\",\n"
            + "      \"value\": \"USA\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"type\": \"eq\",\n"
            + "      \"term\": \"name\",\n"
            + "      \"value\": \"PK\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    AccessControlPolicyContent policy = AccessControlPolicyContent.fromString(jsonContent);

    assertNotNull(policy);
    assertEquals("ANALYST", policy.getPrincipalRole());
    assertEquals(Arrays.asList("name", "location"), policy.getColumnProjections());

    // Validate rowFilters
    assertNotNull(policy.getRowFilters());
    assertEquals(2, policy.getRowFilters().size());

    Expression filter1 = policy.getRowFilters().get(0);
    Expression filter2 = policy.getRowFilters().get(1);
    assertEquals("ref(name=\"country\") == \"USA\"", filter1.toString());
    assertEquals("ref(name=\"name\") == \"PK\"", filter2.toString());
  }

  @Test
  @DisplayName(
      "Should fail deserialize policy with only required fields (principalRole empty, lists empty/null)")
  void testFromString_minimalPolicy() {
    String jsonContent =
        "{\n"
            + "  \"principalRole\": null,\n"
            + "  \"columnProjections\": [],\n"
            + "  \"rowFilters\": []\n"
            + "}";
    assertThrows(
        InvalidPolicyException.class,
        () -> {
          AccessControlPolicyContent.fromString(jsonContent);
        });
  }

  @Test
  void testFromString_missingOptionalFields() {
    String jsonContent = "{\n" + "  \"principalRole\": \"DATA_ENGINEER\"\n" + "}";
    assertThrows(
        InvalidPolicyException.class,
        () -> {
          AccessControlPolicyContent.fromString(jsonContent);
        });
  }

  @Test
  @DisplayName("Should throw InvalidPolicyException for empty content")
  void testFromString_emptyContent() {
    String emptyContent = "";
    InvalidPolicyException exception =
        assertThrows(
            InvalidPolicyException.class,
            () -> {
              AccessControlPolicyContent.fromString(emptyContent);
            });
    assertEquals("Policy is empty", exception.getMessage());
  }

  @Test
  @DisplayName("Should throw InvalidPolicyException for null content")
  void testFromString_nullContent() {
    String nullContent = null;
    InvalidPolicyException exception =
        assertThrows(
            InvalidPolicyException.class,
            () -> {
              AccessControlPolicyContent.fromString(nullContent);
            });
    assertEquals("Policy is empty", exception.getMessage());
  }

  @Test
  @DisplayName("Should throw InvalidPolicyException for invalid JSON content")
  void testFromString_invalidJson() {
    String invalidJson =
        "{ \"principalRole\": \"ANALYST\", \"columnProjections\": [\"name\", }"; // Malformed JSON
    InvalidPolicyException exception =
        assertThrows(
            InvalidPolicyException.class,
            () -> {
              AccessControlPolicyContent.fromString(invalidJson);
            });
    assertNotNull(
        exception.getCause()); // Check that the cause is set (Jackson's JsonParseException)
    assertTrue(exception.getCause() instanceof com.fasterxml.jackson.core.JsonProcessingException);
  }

  @Test
  @DisplayName(
      "Should handle rowFilters with different Iceberg expression types (if supported by parser)")
  void testFromString_differentRowFilterTypes() {
    // This test assumes your ExpressionParser can handle various types.
    // Our dummy only handles 'eq' for now, but in a real scenario, you'd test 'and', 'or', 'gt',
    // etc.
    String jsonContent =
        "{\n"
            + "  \"principalRole\": \"ADMIN\",\n"
            + "  \"rowFilters\": [\n"
            + "    {\n"
            + "      \"type\": \"gt\",\n"
            + // Example of a different type
            "      \"term\": \"age\",\n"
            + "      \"value\": \"30\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"type\": \"eq\",\n"
            + "      \"term\": \"status\",\n"
            + "      \"value\": \"active\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    AccessControlPolicyContent policy = AccessControlPolicyContent.fromString(jsonContent);
    assertNotNull(policy);
    assertEquals(2, policy.getRowFilters().size());
    Expression filter1 = policy.getRowFilters().get(0);
    Expression filter2 = policy.getRowFilters().get(1);
    assertEquals("ref(name=\"age\") > \"30\"", filter1.toString());
    assertEquals("ref(name=\"status\") == \"active\"", filter2.toString());
  }

  @Test
  @DisplayName("Should correctly handle empty lists in JSON")
  void testFromString_emptyListsInJson() {
    String jsonContent =
        "{\n"
            + "  \"principalRole\": \"VIEWER\",\n"
            + "  \"columnProjections\": [],\n"
            + "  \"rowFilters\": []\n"
            + "}";

    assertThrows(
        InvalidPolicyException.class,
        () -> {
          AccessControlPolicyContent.fromString(jsonContent);
        });
  }

  @Test
  @DisplayName(
      "Should handle unmapped properties if FAIL_ON_UNKNOWN_PROPERTIES is false (default for this setup)")
  void testFromString_unmappedProperties() {
    String jsonContent =
        "{\n"
            + "  \"principalRole\": \"ANALYST\",\n"
            + "  \"extraField\": \"someValue\"\n"
            + // Extra field
            "}";
    assertThrows(
        InvalidPolicyException.class,
        () -> {
          AccessControlPolicyContent.fromString(jsonContent);
        });
  }
}
