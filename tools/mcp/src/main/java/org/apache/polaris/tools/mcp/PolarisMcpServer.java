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

package org.apache.polaris.tools.mcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Entry point for the Polaris Model Context Protocol server. */
public final class PolarisMcpServer {
  private static final String DEFAULT_BASE_URL = "http://localhost:8181/";

  private final ObjectMapper mapper;
  private final McpDispatcher dispatcher;

  PolarisMcpServer(ObjectMapper mapper, McpDispatcher dispatcher) {
    this.mapper = mapper;
    this.dispatcher = dispatcher;
  }

  public static void main(String[] args) throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    URI baseUri = URI.create(resolveBaseUrl());
    Optional<String> token = resolveToken();
    HttpExecutor executor = PolarisRestTool.defaultExecutor();

    List<McpTool> tools = new ArrayList<>();
    tools.add(new PolarisTableTool(mapper, executor, baseUri, token));
    tools.add(new PolarisPolicyTool(mapper, executor, baseUri, token));
    String serverVersion =
        Optional.ofNullable(PolarisMcpServer.class.getPackage().getImplementationVersion())
            .orElse("dev");

    McpDispatcher dispatcher = new McpDispatcher(mapper, tools, "polaris-mcp", serverVersion);
    PolarisMcpServer server = new PolarisMcpServer(mapper, dispatcher);
    server.run();
  }

  void run() throws IOException {
    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintWriter writer =
            new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true)) {
      boolean running = true;
      while (running) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        if (line.isBlank()) {
          continue;
        }
        try {
          JsonNode request = mapper.readTree(line);
          McpDispatcher.DispatchResult result = dispatcher.handle(request);
          if (result.response() != null) {
            writer.println(mapper.writeValueAsString(result.response()));
            writer.flush();
          }
          if (result.shouldExit()) {
            running = false;
          }
        } catch (JsonProcessingException parseException) {
          ObjectNode error = buildParseError(parseException.getOriginalMessage());
          writer.println(mapper.writeValueAsString(error));
          writer.flush();
        }
      }
    }
  }

  private ObjectNode buildParseError(String message) {
    ObjectNode response = mapper.createObjectNode();
    response.put("jsonrpc", "2.0");
    response.set("id", mapper.nullNode());
    ObjectNode error = response.putObject("error");
    error.put("code", -32700);
    error.put("message", "Parse error");
    if (message != null && !message.isEmpty()) {
      error.put("data", message);
    }
    return response;
  }

  private static String resolveBaseUrl() {
    return firstNonBlank(
        System.getenv("POLARIS_BASE_URL"),
        System.getenv("POLARIS_REST_BASE_URL"),
        System.getProperty("polaris.baseUrl"),
        DEFAULT_BASE_URL);
  }

  private static Optional<String> resolveToken() {
    String token =
        firstNonBlank(
            System.getenv("POLARIS_API_TOKEN"),
            System.getenv("POLARIS_BEARER_TOKEN"),
            System.getenv("POLARIS_TOKEN"),
            System.getProperty("polaris.apiToken"));
    return Optional.ofNullable(token);
  }

  private static String firstNonBlank(String... candidates) {
    for (String candidate : candidates) {
      if (candidate != null && !candidate.trim().isEmpty()) {
        return candidate.trim();
      }
    }
    return null;
  }
}
