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

import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_VIEW;
import static org.apache.polaris.core.policy.PredefinedPolicyTypes.DATA_COMPACTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;
import org.apache.polaris.core.policy.validator.PolicyValidators;
import org.junit.jupiter.api.Test;

public class PolicyValidatorsTest {
  Namespace ns = Namespace.of("NS1");
  TableIdentifier tableIdentifier = TableIdentifier.of(ns, "table1");
  PolicyEntity policyEntity = new PolicyEntity.Builder(ns, "pn", DATA_COMPACTION).build();

  @Test
  public void testInvalidPolicy() {
    var policyEntity =
        new PolicyEntity.Builder(ns, "testPolicy", DATA_COMPACTION)
            .setContent("InvalidContent")
            .setPolicyVersion(0)
            .build();
    assertThatThrownBy(() -> PolicyValidators.validate(policyEntity))
        .as("Validating empty JSON '{}' should throw InvalidPolicyException")
        .isInstanceOf(InvalidPolicyException.class)
        .hasMessageContaining("Invalid policy");
  }

  @Test
  public void testUnsupportedPolicyType() {
    var newPolicyType =
        new PolicyType() {
          @Override
          public int getCode() {
            return Integer.MAX_VALUE;
          }

          @Override
          public String getName() {
            return "";
          }

          @Override
          public boolean isInheritable() {
            return false;
          }
        };

    var policyEntity = new PolicyEntity.Builder(ns, "testPolicy", newPolicyType).build();

    assertThatThrownBy(() -> PolicyValidators.validate(policyEntity))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown policy type:");
  }

  @Test
  public void testValidPolicy() {
    var policyEntity =
        new PolicyEntity.Builder(ns, "testPolicy", DATA_COMPACTION)
            .setContent("{\"enable\": false}")
            .setPolicyVersion(0)
            .build();
    PolicyValidators.validate(policyEntity);
  }

  @Test
  public void testCanAttachReturnsTrueForCatalogType() {
    var targetEntity = new CatalogEntity.Builder().build();
    var result = PolicyValidators.canAttach(policyEntity, targetEntity);
    assertThat(result).isTrue().as("Expected canAttach() to return true for CATALOG type");
  }

  @Test
  public void testCanAttachReturnsTrueForNamespaceType() {
    var targetEntity = new NamespaceEntity.Builder(ns).build();
    var result = PolicyValidators.canAttach(policyEntity, targetEntity);
    assertThat(result).isTrue().as("Expected canAttach() to return true for CATALOG type");
  }

  @Test
  public void testCanAttachReturnsTrueForIcebergTableLikeWithTableSubtype() {
    var targetEntity =
        new IcebergTableLikeEntity.Builder(ICEBERG_TABLE, tableIdentifier, "").build();
    var result = PolicyValidators.canAttach(policyEntity, targetEntity);
    assertThat(result)
        .isTrue()
        .as("Expected canAttach() to return true for ICEBERG_TABLE_LIKE with TABLE subtype");
  }

  @Test
  public void testCanAttachReturnsFalseForIcebergTableLikeWithNonTableSubtype() {
    var targetEntity =
        new IcebergTableLikeEntity.Builder(ICEBERG_VIEW, tableIdentifier, "").build();
    var result = PolicyValidators.canAttach(policyEntity, targetEntity);
    assertThat(result)
        .isFalse()
        .as("Expected canAttach() to return false for ICEBERG_TABLE_LIKE with non-TABLE subtype");
  }

  @Test
  public void testCanAttachReturnsFalseForUnattachableType() {
    var targetEntity = new PrincipalEntity.Builder().build();
    var result = PolicyValidators.canAttach(policyEntity, targetEntity);
    assertThat(result).isFalse().as("Expected canAttach() to return false for null");
  }
}
