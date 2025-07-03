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
package org.apache.polaris.delegation.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;

/**
 * Table identity information for delegation tasks.
 *
 * <p>Provides the complete hierarchical path to identify a table within the Polaris catalog system.
 */
public class TableIdentity {

  @NotNull private final String catalogName;

  @NotNull private final List<String> namespaceLevels;

  @NotNull private final String tableName;

  @JsonCreator
  public TableIdentity(
      @JsonProperty("catalog_name") @NotNull String catalogName,
      @JsonProperty("namespace_levels") @NotNull List<String> namespaceLevels,
      @JsonProperty("table_name") @NotNull String tableName) {
    this.catalogName = catalogName;
    this.namespaceLevels = namespaceLevels;
    this.tableName = tableName;
  }

  @JsonProperty("catalog_name")
  public String getCatalogName() {
    return catalogName;
  }

  @JsonProperty("namespace_levels")
  public List<String> getNamespaceLevels() {
    return namespaceLevels;
  }

  @JsonProperty("table_name")
  public String getTableName() {
    return tableName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableIdentity that = (TableIdentity) o;
    return Objects.equals(catalogName, that.catalogName)
        && Objects.equals(namespaceLevels, that.namespaceLevels)
        && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalogName, namespaceLevels, tableName);
  }

  @Override
  public String toString() {
    return "TableIdentity{"
        + "catalogName='"
        + catalogName
        + '\''
        + ", namespaceLevels="
        + namespaceLevels
        + ", tableName='"
        + tableName
        + '\''
        + '}';
  }
}
