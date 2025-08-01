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
package org.apache.polaris.delegation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import jakarta.ws.rs.ApplicationPath;
import jakarta.ws.rs.core.Application;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.polaris.delegation.api.DelegationApi;
import org.apache.polaris.delegation.api.model.TaskExecutionRequest;
import org.apache.polaris.delegation.api.model.TaskExecutionResponse;
import org.apache.polaris.delegation.service.TaskExecutionService;
import org.apache.polaris.delegation.service.storage.StorageFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for the Polaris Delegation Service.
 *
 * <p>This service handles long-running tasks delegated from the main Polaris catalog to maintain
 * low-latency performance in the catalog operations.
 */
@ApplicationPath("/")
public class DelegationServiceApplication extends Application {

  private static final Logger LOGGER = LoggerFactory.getLogger(DelegationServiceApplication.class);

  @Override
  public Set<Class<?>> getClasses() {
    LOGGER.info("Initializing Polaris Delegation Service");

    // Return the set of JAX-RS resource classes
    return Set.of(
        DelegationApi.class,
        // Add other resource classes here as needed
        JacksonJsonProvider.class // For JSON serialization
        );
  }

  /**
   * Main entry point for standalone execution.
   *
   * <p>This method can be used to run the delegation service as a standalone application using an
   * embedded server like Jetty or Undertow.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    LOGGER.info("Starting Polaris Delegation Service...");

    // Get configuration from command line arguments first, then environment, then defaults
    String port = "8282";
    String host = "localhost";

    // Parse command line arguments
    if (args.length > 0) {
      port = args[0];
    } else {
      // Fallback to environment variables
      port = System.getenv().getOrDefault("POLARIS_DELEGATION_PORT", "8282");
    }

    if (args.length > 1) {
      host = args[1];
    } else {
      // Fallback to environment variables
      host = System.getenv().getOrDefault("POLARIS_DELEGATION_HOST", "localhost");
    }

    LOGGER.info("Delegation Service Configuration:");
    LOGGER.info("  - Host: {}", host);
    LOGGER.info("  - Port: {}", port);
    LOGGER.info("  - Base URL: http://{}:{}", host, port);

    try {
      // For POC, we'll use a simple embedded server
      // In production, this would be deployed to a proper application server
      startEmbeddedServer(host, Integer.parseInt(port));

    } catch (Exception e) {
      LOGGER.error("Failed to start Polaris Delegation Service", e);
      System.exit(1);
    }
  }

  /**
   * Starts an embedded server for POC demonstration. In production, this would be replaced with
   * proper deployment to an application server.
   */
  private static void startEmbeddedServer(String host, int port) throws Exception {
    LOGGER.info("Starting embedded server for POC demonstration...");

    // Create virtual thread executor for concurrent request handling
    ExecutorService requestExecutor = Executors.newVirtualThreadPerTaskExecutor();

    // Create actual service instances
    StorageFileManager storageFileManager = new StorageFileManager();
    TaskExecutionService taskExecutionService = new TaskExecutionService();
    ObjectMapper objectMapper = new ObjectMapper();

    // Configure ObjectMapper to handle JSR310 date/time types
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    // Manually inject dependencies (in production, this would be handled by CDI)
    // Use reflection to set the private field
    java.lang.reflect.Field field =
        TaskExecutionService.class.getDeclaredField("storageFileManager");
    field.setAccessible(true);
    field.set(taskExecutionService, storageFileManager);

    // Create a simple HTTP server using Java's built-in HTTP server
    com.sun.net.httpserver.HttpServer server =
        com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(host, port), 0);

    // Configure the server to use virtual threads for handling HTTP connections
    // This allows multiple concurrent HTTP requests to be processed simultaneously
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

    // Create a simple context for health checks
    server.createContext(
        "/health",
        exchange -> {
          String response = "{\"status\":\"healthy\",\"service\":\"polaris-delegation-service\"}";
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);
          exchange.getResponseBody().write(response.getBytes(StandardCharsets.UTF_8));
          exchange.close();
        });

    // Create the actual API endpoint using virtual threads for concurrent request handling
    server.createContext(
        "/api/v1/tasks/execute/synchronous",
        exchange -> {
          // Handle each request in a virtual thread but wait for completion (synchronous response)
          try {
            CompletableFuture<Void> requestFuture =
                CompletableFuture.runAsync(
                    () -> {
                      handleDelegationRequest(exchange, objectMapper, taskExecutionService);
                    },
                    requestExecutor);

            // Wait for the request to complete before returning (maintains synchronous contract)
            requestFuture.get();
          } catch (Exception e) {
            LOGGER.error("Error in request processing", e);
            try {
              String errorResponse =
                  "{\"status\":\"failed\",\"result_summary\":\"Internal server error\"}";
              exchange.getResponseHeaders().add("Content-Type", "application/json");
              exchange.sendResponseHeaders(
                  500, errorResponse.getBytes(StandardCharsets.UTF_8).length);
              exchange.getResponseBody().write(errorResponse.getBytes(StandardCharsets.UTF_8));
              exchange.close();
            } catch (Exception responseError) {
              LOGGER.error("Failed to send error response", responseError);
            }
          }
        });

    // Start the server
    server.start();

    LOGGER.info("Polaris Delegation Service started successfully!");
    LOGGER.info("   Health check: http://{}:{}/health", host, port);
    LOGGER.info("   API endpoint: http://{}:{}/api/v1/tasks/execute/synchronous", host, port);
    LOGGER.info("   Press Ctrl+C to stop the service");

    // Keep the server running
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOGGER.info("Shutting down Polaris Delegation Service...");
                  requestExecutor.shutdown();
                  try {
                    if (!requestExecutor.awaitTermination(
                        30, java.util.concurrent.TimeUnit.SECONDS)) {
                      LOGGER.warn(
                          "Request executor did not terminate gracefully, forcing shutdown");
                      requestExecutor.shutdownNow();
                    }
                  } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted while waiting for request executor shutdown");
                    requestExecutor.shutdownNow();
                  }
                  server.stop(5);
                  LOGGER.info("Delegation Service stopped.");
                }));

    // Wait for shutdown
    Thread.currentThread().join();
  }

  /**
   * Handles a single delegation request. This method contains all the request processing logic and
   * runs in a virtual thread for concurrent request handling.
   */
  private static void handleDelegationRequest(
      HttpExchange exchange, ObjectMapper objectMapper, TaskExecutionService taskExecutionService) {

    LOGGER.info("DELEGATION REQUEST RECEIVED!");
    LOGGER.info("   Method: {}", exchange.getRequestMethod());
    LOGGER.info("   URI: {}", exchange.getRequestURI());
    LOGGER.info("   Headers: {}", exchange.getRequestHeaders());

    if (!"POST".equals(exchange.getRequestMethod())) {
      LOGGER.warn("   Method not allowed: {}", exchange.getRequestMethod());
      try {
        exchange.sendResponseHeaders(405, -1);
        exchange.close();
      } catch (Exception e) {
        LOGGER.error("Failed to send 405 response", e);
      }
      return;
    }

    try {
      // Read request body
      String requestBody =
          new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      LOGGER.info("   Request body: {}", requestBody);

      // Parse request using Jackson
      TaskExecutionRequest request =
          objectMapper.readValue(requestBody, TaskExecutionRequest.class);

      // Execute the task using the real implementation
      TaskExecutionResponse response = taskExecutionService.executeTask(request);

      // Serialize response to JSON
      String responseJson = objectMapper.writeValueAsString(response);

      // Determine HTTP status code and log message based on task execution result
      boolean isSuccess = "success".equalsIgnoreCase(response.getStatus());
      int httpStatusCode;

      if (isSuccess) {
        httpStatusCode = 200; // OK - Task completed successfully
        LOGGER.info("   Task executed successfully. Response: {}", responseJson);
      } else {
        // Determine if this is a client error (4xx) or server error (5xx)
        String resultSummary = response.getResultSummary();
        if (resultSummary != null
            && (resultSummary.contains("does not exist")
                || resultSummary.contains("NoSuchTableException")
                || resultSummary.contains("404"))) {
          httpStatusCode = 422; // Unprocessable Entity - Valid request, but table doesn't exist
        } else {
          httpStatusCode = 500; // Internal Server Error - Unexpected failure
        }
        LOGGER.warn("   Task execution failed. Response: {}", responseJson);
      }

      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(
          httpStatusCode, responseJson.getBytes(StandardCharsets.UTF_8).length);
      exchange.getResponseBody().write(responseJson.getBytes(StandardCharsets.UTF_8));
      exchange.close();

    } catch (Exception e) {
      LOGGER.error("   Error processing request", e);

      try {
        String errorResponse =
            "{\"status\":\"failed\",\"result_summary\":\"Error processing request: "
                + e.getMessage()
                + "\"}";
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(500, errorResponse.getBytes(StandardCharsets.UTF_8).length);
        exchange.getResponseBody().write(errorResponse.getBytes(StandardCharsets.UTF_8));
        exchange.close();
      } catch (Exception responseError) {
        LOGGER.error("Failed to send error response", responseError);
      }
    }
  }

  /**
   * Jackson JSON provider for JAX-RS. This would normally be provided by the application server or
   * framework.
   */
  public static class JacksonJsonProvider {
    // Placeholder - in a real application, this would be configured properly
    // For POC, we'll use the simple embedded server approach above
  }
}
