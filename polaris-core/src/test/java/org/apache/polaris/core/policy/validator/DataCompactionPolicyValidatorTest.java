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
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_VIEW;
import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG;
import static org.apache.polaris.core.entity.PolarisEntityType.NAMESPACE;
import static org.apache.polaris.core.entity.PolarisEntityType.PRINCIPAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.policy.validator.datacompaction.DataCompactionPolicyValidator;
import org.junit.jupiter.api.Test;

public class DataCompactionPolicyValidatorTest {
  private final DataCompactionPolicyValidator validator = new DataCompactionPolicyValidator();

  @Test
  public void testValidPolicies() {
    var validJson = "{\"version\":\"2025-02-03\", \"enable\": true}";
    validator.validate(validJson);

    assertThatThrownBy(() -> validator.validate(""))
        .as("Validating empty string should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Policy is empty");
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
    var result = validator.canAttach(PolarisEntityType.TABLE_LIKE, ICEBERG_TABLE);
    assertThat(result)
        .isTrue()
        .as("Expected canAttach() to return true for ICEBERG_TABLE_LIKE with TABLE subtype");
  }

  @Test
  public void testCanAttachReturnsFalseForIcebergTableLikeWithNonTableSubtype() {
    // For ICEBERG_TABLE_LIKE, any subtype other than TABLE should return false.
    boolean result = validator.canAttach(PolarisEntityType.TABLE_LIKE, ICEBERG_VIEW);
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
