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
package org.apache.polaris.core.policy.validator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class DataCompactionPolicyValidatorTest {
  private final DataCompactionPolicyValidator validator = new DataCompactionPolicyValidator();

  @Test
  public void testValidPolicies() {
    validator.parse("{\"enable\": false}");
    validator.parse("{\"enable\": true}");

    var validJson = "{\"version\":\"2025-02-03\", \"enable\": true}";
    validator.parse(validJson);

    validJson = "{\"enable\": true, \"config\": {\"key1\": \"value1\", \"key2\": true}}";
    validator.parse(validJson);
  }

  @Test
  void testParseEmptyString() {
    assertThatThrownBy(() -> validator.parse(""))
        .as("Validating empty string should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Policy is empty");
  }

  @Test
  void testParseEmptyJson() {
    assertThatThrownBy(() -> validator.parse("{}"))
        .as("Validating empty JSON '{}' should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  void testParseInvalidVersionFormat() {
    String invalidPolicy1 = "{\"enable\": true, \"version\": \"fdafds\"}";
    assertThatThrownBy(() -> validator.parse(invalidPolicy1))
        .as("Validating policy with invalid version format should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class);
  }

  @Test
  void testParseInvalidKeyInPolicy() {
    String invalidPolicy2 =
        "{\"version\":\"2025-02-03\", \"enable\": true, \"invalid_key\": 12342}";
    assertThatThrownBy(() -> validator.parse(invalidPolicy2))
        .as("Validating policy with an unknown key should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  void testParseUnrecognizedToken() {
    var invalidPolicy = "{\"enable\": invalidToken}";
    assertThatThrownBy(() -> validator.parse(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  void testParseNullValue() {
    var invalidPolicy = "{\"enable\": null}";
    assertThatThrownBy(() -> validator.parse(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  void testParseWrongString() {
    var invalidPolicy = "{\"enable\": \"invalid\"}";
    assertThatThrownBy(() -> validator.parse(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }
}
