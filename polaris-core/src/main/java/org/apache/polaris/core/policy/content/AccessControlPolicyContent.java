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

package org.apache.polaris.core.policy.content;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;

public class AccessControlPolicyContent implements PolicyContent {

  // Optional, if there means policies is applicable to the role
  private String principalRole;

  // TODO: model them as iceberg transforms
  private List<String> columnProjections;

  // Iceberg expressions without context functions for now.
  // Use a custom deserializer for the list of Iceberg Expressions
  @JsonDeserialize(using = IcebergExpressionListDeserializer.class)
  @JsonSerialize(using = IcebergExpressionListSerializer.class)
  private List<Expression> rowFilters;

  private static final String DEFAULT_POLICY_SCHEMA_VERSION = "2025-02-03";
  private static final Set<String> POLICY_SCHEMA_VERSIONS = Set.of(DEFAULT_POLICY_SCHEMA_VERSION);

  public static AccessControlPolicyContent fromString(String content) {
    if (Strings.isNullOrEmpty(content)) {
      throw new InvalidPolicyException("Policy is empty");
    }

    AccessControlPolicyContent policy;
    try {
      policy = PolicyContentUtil.MAPPER.readValue(content, AccessControlPolicyContent.class);
    } catch (Exception e) {
      throw new InvalidPolicyException(e);
    }

    boolean isProjectionsEmpty =
        policy.getColumnProjections() == null || policy.getColumnProjections().isEmpty();
    boolean isRowFilterEmpty = policy.getRowFilters() == null || policy.getRowFilters().isEmpty();
    if (isProjectionsEmpty && isRowFilterEmpty) {
      throw new InvalidPolicyException("Policy must contain 'columnProjections' or 'rowFilters'.");
    }

    return policy;
  }

  public static String toString(AccessControlPolicyContent content) {
    if (content == null) {
      return null;
    }
    try {
      return PolicyContentUtil.MAPPER.writeValueAsString(content);
    } catch (JsonProcessingException e) {
      throw new InvalidPolicyException("Failed to convert policy content to JSON string", e);
    }
  }

  // Constructors, getters, and setters
  public AccessControlPolicyContent() {}

  public String getPrincipalRole() {
    return principalRole;
  }

  public void setPrincipalRole(String principalRole) {
    this.principalRole = principalRole;
  }

  public List<String> getColumnProjections() {
    return columnProjections;
  }

  public void setAllowedColumns(List<String> columnProjections) {
    this.columnProjections = columnProjections;
  }

  public List<Expression> getRowFilters() {
    return rowFilters;
  }

  public void setRowFilters(List<Expression> rowFilters) {
    this.rowFilters = rowFilters;
  }

  @Override
  public String toString() {
    return "AccessControlPolicyContent{"
        + "principalRole='"
        + principalRole
        + '\''
        + ", columnProjections="
        + columnProjections
        + ", rowFilters="
        + rowFilters
        + '}';
  }
}
