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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base class for operation-specific parameters in delegation tasks.
 *
 * <p>This abstract class serves as the base for all operation-specific parameter types. Each
 * concrete subclass represents parameters for a specific type of delegation operation, providing
 * type safety and clear separation of concerns.
 *
 * <p>Uses Jackson polymorphism to serialize/deserialize different parameter types based on the
 * operation type. This allows the API to handle different parameter structures while maintaining
 * type safety.
 *
 * <p><strong>Supported Operation Types:</strong>
 *
 * <ul>
 *   <li>{@link TablePurgeParameters} - For TABLE_PURGE operations
 * </ul>
 *
 * <p><strong>Adding New Operation Types:</strong>
 *
 * <ol>
 *   <li>Create a new concrete subclass extending {@code OperationParameters}
 *   <li>Add the subclass to the {@link JsonSubTypes} annotation
 *   <li>Define the operation-specific fields and validation
 * </ol>
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "task_type")
@JsonSubTypes({@JsonSubTypes.Type(value = TablePurgeParameters.class, name = "PURGE_TABLE")})
public abstract class OperationParameters {

  /**
   * Gets the task type that these parameters support.
   *
   * @return the task type
   */
  public abstract TaskType getTaskType();
}
