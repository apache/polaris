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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.delegation.api.model.CommonPayload;
import org.apache.polaris.delegation.api.model.TableIdentity;
import org.apache.polaris.delegation.api.model.TablePurgeParameters;
import org.apache.polaris.delegation.api.model.TaskExecutionRequest;
import org.apache.polaris.delegation.api.model.TaskExecutionResponse;
import org.apache.polaris.delegation.api.model.TaskType;
import org.apache.polaris.service.config.DelegationServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of DelegationClient that communicates with the delegation service via HTTP.
 *
 * <p>This client converts Polaris internal task entities to delegation service API contracts and
 * handles HTTP communication with the delegation service.
 */
public class DelegationClientImpl implements DelegationClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DelegationClientImpl.class);

  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final DelegationServiceConfiguration config;

  public DelegationClientImpl(
      HttpClient httpClient, ObjectMapper objectMapper, DelegationServiceConfiguration config) {
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
    this.config = config;
    
    try {
      config.validate();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid delegation service configuration: {}", e.getMessage());
      throw e;
    }
  }

  @Override
  public boolean delegateTask(TaskEntity task, CallContext callContext) throws DelegationException {
    if (!canDelegate(task, callContext)) {
      return false;
    }

    try {
      TaskExecutionRequest request = convertToRequest(task, callContext);

      String requestBody = objectMapper.writeValueAsString(request);
      HttpRequest httpRequest =
          HttpRequest.newBuilder()
              .uri(URI.create(config.getBaseUrl() + "/api/v1/tasks/execute/synchronous"))
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
              .POST(HttpRequest.BodyPublishers.ofString(requestBody))
              .build();

      LOGGER.info(
          "Delegating task {} to delegation service at {}", task.getId(), config.getBaseUrl());

      HttpResponse<String> response =
          httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        TaskExecutionResponse delegationResponse =
            objectMapper.readValue(response.body(), TaskExecutionResponse.class);
        LOGGER.info(
            "Task {} delegated successfully: status={}, summary={}",
            task.getId(),
            delegationResponse.getStatus(),
            delegationResponse.getResultSummary());
        return "COMPLETED".equals(delegationResponse.getStatus());
      } else {
        throw new DelegationException(
            String.format(
                "Delegation service returned error: %d %s",
                response.statusCode(), response.body()));
      }

    } catch (IOException | InterruptedException e) {
      throw new DelegationException("Failed to communicate with delegation service", e);
    }
  }

  @Override
  public boolean isHealthy() {
    if (!config.isEnabled()) {
      return false;
    }

    try {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(config.getBaseUrl() + "/health"))
          .timeout(Duration.ofSeconds(5))
          .GET().build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      return response.statusCode() == 200;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean delegatePurge(
      String catalogName,
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      Map<String, String> storageProperties,
      CallContext callContext) {

    if (!isDelegationEnabled()) {
      LOGGER.debug("Delegation service is not enabled for table {}", tableIdentifier);
      return false;
    }

    try {
      LOGGER.info(
          "Sending synchronous request to delegation service for data file cleanup of table {}",
          tableIdentifier);

      boolean delegationSuccessful =
          delegateTablePurge(catalogName, tableIdentifier, storageProperties, callContext);

      if (delegationSuccessful) {
        LOGGER.info(
            "Delegation service SYNCHRONOUSLY COMPLETED data file cleanup for table {}",
            tableIdentifier);
        return true;
      } else {
        LOGGER.error(
            "Delegation service failed to complete data file cleanup for table {}",
            tableIdentifier);
        return false;
      }

    } catch (Exception e) {
      LOGGER.error("Error during delegation for table {}", tableIdentifier, e);
      return false;
    }
  }

  /** Delegates the actual purge operation to the delegation service. */
  private boolean delegateTablePurge(
      String catalogName,
      TableIdentifier tableIdentifier,
      Map<String, String> storageProperties,
      CallContext callContext)
      throws DelegationException {

    try {
      CommonPayload commonPayload =
          new CommonPayload(
              TaskType.PURGE_TABLE,
              OffsetDateTime.now(),
              callContext.getRealmContext().getRealmIdentifier());

      TableIdentity tableIdentity =
          new TableIdentity(
              catalogName,
              Arrays.asList(tableIdentifier.namespace().levels()),
              tableIdentifier.name());

      TablePurgeParameters operationParameters = new TablePurgeParameters(tableIdentity);
      TaskExecutionRequest request = new TaskExecutionRequest(commonPayload, operationParameters);

      String requestBody = objectMapper.writeValueAsString(request);
      HttpRequest httpRequest =
          HttpRequest.newBuilder()
              .uri(URI.create(config.getBaseUrl() + "/api/v1/tasks/execute/synchronous"))
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .timeout(Duration.ofSeconds(config.getTimeoutSeconds()))
              .POST(HttpRequest.BodyPublishers.ofString(requestBody))
              .build();

      LOGGER.info(
          "Sending BLOCKING HTTP request to delegation service at {} for table {}",
          config.getBaseUrl(),
          tableIdentifier);

      HttpResponse<String> response =
          httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        TaskExecutionResponse delegationResponse =
            objectMapper.readValue(response.body(), TaskExecutionResponse.class);
        LOGGER.info(
            "Delegation service responded: status={}, summary={}",
            delegationResponse.getStatus(),
            delegationResponse.getResultSummary());

        boolean success = "success".equals(delegationResponse.getStatus());
        if (success) {
          LOGGER.info(
              "Delegation service completed data file cleanup for table {}", tableIdentifier);
        } else {
          LOGGER.error(
              "Delegation service reported failure for table {}: {}",
              tableIdentifier,
              delegationResponse.getResultSummary());
        }
        return success;
      } else {
        LOGGER.error(
            "Delegation service returned HTTP error: {} {} for table {}",
            response.statusCode(),
            response.body(),
            tableIdentifier);
        return false;
      }

    } catch (IOException | InterruptedException e) {
      throw new DelegationException("Failed to communicate with delegation service", e);
    }
  }

  private boolean isDelegationEnabled() {
    String delegationEnabled =
        System.getProperty(
            "polaris.delegation.enabled",
            System.getenv().getOrDefault("POLARIS_DELEGATION_ENABLED", "false"));
    return "true".equalsIgnoreCase(delegationEnabled) && config.isEnabled();
  }

  @Override
  public boolean canDelegate(TaskEntity task, CallContext callContext) {
    if (!config.isEnabled()) {
      return false;
    }

    AsyncTaskType taskType = task.getTaskType();
    if (taskType != AsyncTaskType.ENTITY_CLEANUP_SCHEDULER) {
      return false;
    }

    try {
      PolarisBaseEntity entity = task.readData(PolarisBaseEntity.class);
      return IcebergTableLikeEntity.of(entity) != null;
    } catch (Exception e) {
      LOGGER.warn("Failed to read task data for delegation check", e);
      return false;
    }
  }

  private TaskExecutionRequest convertToRequest(TaskEntity task, CallContext callContext)
      throws DelegationException {
    try {
      IcebergTableLikeEntity tableEntity = task.readData(IcebergTableLikeEntity.class);
      if (tableEntity == null) {
        throw new DelegationException("Task does not contain table entity data");
      }

      CommonPayload commonPayload =
          new CommonPayload(
              TaskType.PURGE_TABLE,
              OffsetDateTime.now(),
              callContext.getRealmContext().getRealmIdentifier());

      TableIdentifier tableId = tableEntity.getTableIdentifier();
      TableIdentity tableIdentity =
          new TableIdentity(
              tableId.namespace().level(0),
              Arrays.asList(tableId.namespace().levels()).subList(1, tableId.namespace().length()),
              tableId.name());

      TablePurgeParameters operationParameters = new TablePurgeParameters(tableIdentity);

      return new TaskExecutionRequest(commonPayload, operationParameters);

    } catch (Exception e) {
      throw new DelegationException("Failed to convert task to delegation request", e);
    }
  }
}
