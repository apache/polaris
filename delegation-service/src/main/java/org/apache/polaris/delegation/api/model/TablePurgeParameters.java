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
import java.util.Map;
import java.util.Objects;

/**
 * Operation parameters for table purge operations.
 *
 * <p>Contains all the information needed to perform a table purge operation, including the table
 * identity and optional configuration properties.
 *
 * <p>This class represents parameters for {@code PURGE_TABLE} operations, which handle the deletion
 * of data files associated with dropped tables.
 *
 * <p><strong>Example Usage:</strong>
 *
 * <pre>
 * TablePurgeParameters params = new TablePurgeParameters(
 *     new TableIdentity("catalog", List.of("namespace"), "table"),
 *     Map.of("skipTrash", "true", "batchSize", "1000")
 * );
 * </pre>
 */
public class TablePurgeParameters extends OperationParameters {

  @NotNull private final TableIdentity tableIdentity;

  private final Map<String, String> properties;

  @JsonCreator
  public TablePurgeParameters(
      @JsonProperty("table_identity") @NotNull TableIdentity tableIdentity,
      @JsonProperty("properties") Map<String, String> properties) {
    this.tableIdentity = tableIdentity;
    this.properties = properties;
  }

  @Override
  public TaskType getTaskType() {
    return TaskType.PURGE_TABLE;
  }

  @JsonProperty("table_identity")
  @NotNull
  public TableIdentity getTableIdentity() {
    return tableIdentity;
  }

  @JsonProperty("properties")
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Gets a property value by key.
   *
   * @param key the property key
   * @return the property value, or null if not found
   */
  public String getProperty(String key) {
    return properties != null ? properties.get(key) : null;
  }

  /**
   * Checks if a property is set to "true".
   *
   * @param key the property key
   * @return true if the property exists and equals "true" (case-insensitive)
   */
  public boolean getBooleanProperty(String key) {
    String value = getProperty(key);
    return "true".equalsIgnoreCase(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TablePurgeParameters that = (TablePurgeParameters) o;
    return Objects.equals(tableIdentity, that.tableIdentity)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableIdentity, properties);
  }

  @Override
  public String toString() {
    return "TablePurgeParameters{"
        + "tableIdentity="
        + tableIdentity
        + ", properties="
        + properties
        + '}';
  }
}
