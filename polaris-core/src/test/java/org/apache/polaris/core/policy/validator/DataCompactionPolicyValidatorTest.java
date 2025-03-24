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

import static org.apache.polaris.core.entity.PolarisEntitySubType.ANY_SUBTYPE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.TABLE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.VIEW;
import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG;
import static org.apache.polaris.core.entity.PolarisEntityType.ICEBERG_TABLE_LIKE;
import static org.apache.polaris.core.entity.PolarisEntityType.NAMESPACE;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.polaris.core.policy.validator.datacompaction.DataCompactionPolicyValidator;
import org.junit.jupiter.api.Test;

public class DataCompactionPolicyValidatorTest {
  private final DataCompactionPolicyValidator validator = new DataCompactionPolicyValidator();

  @Test
  public void testValidPolicies() {
    assertThat(validator.parse("{\"enable\": false}").enabled()).isFalse();
    assertThat(validator.parse("{\"enable\": true}").enabled()).isTrue();

    var validJson = "{\"version\":\"2025-02-03\", \"enable\": true}";
    assertThat(validator.parse(validJson).getVersion()).isEqualTo("2025-02-03");

    validJson = "{\"enable\": true, \"config\": {\"key1\": \"value1\", \"key2\": true}}";
    assertThat(validator.parse(validJson).getConfig().get("key1")).isEqualTo("value1");
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

  @Test
  public void testCanAttachReturnsTrueForCatalogType() {
    var result = validator.canAttach(CATALOG, ANY_SUBTYPE); // using any valid subtype
    assertThat(result).isTrue().as("Expected canAttach() to return true for CATALOG type");
  }

  @Test
  public void testCanAttachReturnsTrueForNamespaceType() {
    var result = validator.canAttach(NAMESPACE, ANY_SUBTYPE); // using any valid subtype
    assertThat(result).isTrue().as("Expected canAttach() to return true for CATALOG type");
  }

  @Test
  public void testCanAttachReturnsTrueForIcebergTableLikeWithTableSubtype() {
    var result = validator.canAttach(ICEBERG_TABLE_LIKE, TABLE);
    assertThat(result)
        .isTrue()
        .as("Expected canAttach() to return true for ICEBERG_TABLE_LIKE with TABLE subtype");
  }

  @Test
  public void testCanAttachReturnsFalseForIcebergTableLikeWithNonTableSubtype() {
    // For ICEBERG_TABLE_LIKE, any subtype other than TABLE should return false.
    boolean result = validator.canAttach(ICEBERG_TABLE_LIKE, VIEW);
    assertThat(result)
        .isFalse()
        .as("Expected canAttach() to return false for ICEBERG_TABLE_LIKE with non-TABLE subtype");
  }

  @Test
  public void testCanAttachReturnsFalseForNull() {
    var result = validator.canAttach(null, null); // using any valid subtype
    assertThat(result).isFalse().as("Expected canAttach() to return false for null");
  }

  @Test
  public void testCanAttachReturnsFalseForUnattachableType() {
    var result = validator.canAttach(PRINCIPAL, null); // using any valid subtype
    assertThat(result).isFalse().as("Expected canAttach() to return false for null");
  }
}
