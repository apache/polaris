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

import static org.apache.polaris.core.policy.validator.datacompaction.DataCompactionPolicyContent.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class DataCompactionPolicyContentTest {
  @Test
  public void testValidPolicies() {
    assertThat(fromString("{\"enable\": false}").enabled()).isFalse();
    assertThat(fromString("{\"enable\": true}").enabled()).isTrue();

    var validJson = "{\"version\":\"2025-02-03\", \"enable\": true}";
    assertThat(fromString(validJson).getVersion()).isEqualTo("2025-02-03");

    validJson = "{\"enable\": true, \"config\": {\"key1\": \"value1\", \"key2\": true}}";
    assertThat(fromString(validJson).getConfig().get("key1")).isEqualTo("value1");
  }

  @Test
  void testIsValidEmptyString() {
    assertThatThrownBy(() -> fromString(""))
        .as("Validating empty string should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Policy is empty");
  }

  @Test
  void testIsValidEmptyJson() {
    assertThatThrownBy(() -> fromString("{}"))
        .as("Validating empty JSON '{}' should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  void testIsValidInvalidVersionFormat() {
    String invalidPolicy1 = "{\"enable\": true, \"version\": \"fdafds\"}";
    assertThatThrownBy(() -> fromString(invalidPolicy1))
        .as("Validating policy with invalid version format should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class);
  }

  @Test
  void testIsValidInvalidKeyInPolicy() {
    String invalidPolicy2 =
        "{\"version\":\"2025-02-03\", \"enable\": true, \"invalid_key\": 12342}";
    assertThatThrownBy(() -> fromString(invalidPolicy2))
        .as("Validating policy with an unknown key should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  void testIsValidUnrecognizedToken() {
    var invalidPolicy = "{\"enable\": invalidToken}";
    assertThatThrownBy(() -> fromString(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  void testIsValidNullValue() {
    var invalidPolicy = "{\"enable\": null}";
    assertThatThrownBy(() -> fromString(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  void testIsValidWrongString() {
    var invalidPolicy = "{\"enable\": \"invalid\"}";
    assertThatThrownBy(() -> fromString(invalidPolicy))
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }
}
