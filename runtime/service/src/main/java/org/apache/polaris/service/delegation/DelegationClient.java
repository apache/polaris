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
package org.apache.polaris.service.delegation;

import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.TaskEntity;

/**
 * Client for interacting with the Polaris Delegation Service.
 *
 * <p>This client is responsible for delegating long-running tasks from the main Polaris catalog
 * service to the delegation service for execution, maintaining low-latency performance in the
 * catalog.
 *
 * <p>The client handles all delegation orchestration including decision-making, HTTP communication,
 * fallback to local execution, and proper operation ordering.
 */
public interface DelegationClient {

  /**
   * Delegate a task to the delegation service for synchronous execution.
   *
   * @param task the task entity to delegate
   * @param callContext the call context for the operation
   * @return true if the task was successfully delegated and completed, false otherwise
   * @throws DelegationException if delegation fails due to communication or service errors
   */
  boolean delegateTask(TaskEntity task, CallContext callContext) throws DelegationException;

  /**
   * Delegates table data file cleanup to the delegation service.
   *
   * <p>Note: This only handles data file deletion from storage. Metadata drop from the catalog is
   * still handled by Polaris after successful delegation.
   *
   * @param catalogName the name of the catalog
   * @param tableIdentifier the identifier of the table to purge
   * @param tableMetadata the metadata of the table to purge
   * @param storageProperties storage configuration properties
   * @param callContext the call context
   * @return true if delegation is enabled and data cleanup was COMPLETED successfully, false if
   *     delegation disabled or failed
   */
  boolean delegatePurge(
      String catalogName,
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      Map<String, String> storageProperties,
      CallContext callContext);

  /**
   * Check if delegation is available and enabled for the given task type.
   *
   * @param task the task to check
   * @param callContext the call context
   * @return true if the task can be delegated, false if it should be executed locally
   */
  boolean canDelegate(TaskEntity task, CallContext callContext);

  /**
   * Checks the health of the delegation service.
   *
   * @return true if the delegation service is healthy and responsive, false otherwise
   */
  boolean isHealthy();
}
