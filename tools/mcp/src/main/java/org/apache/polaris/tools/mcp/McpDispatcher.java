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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Handles JSON-RPC requests for the Polaris MCP server. */
final class McpDispatcher {
  private static final String JSON_RPC_VERSION = "2.0";

  private final ObjectMapper mapper;
  private final List<McpTool> tools;
  private final String serverName;
  private final String serverVersion;

  McpDispatcher(ObjectMapper mapper, List<McpTool> tools, String serverName, String serverVersion) {
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.tools = List.copyOf(Objects.requireNonNull(tools, "tools must not be null"));
    this.serverName = Objects.requireNonNull(serverName, "serverName must not be null");
    this.serverVersion = Objects.requireNonNull(serverVersion, "serverVersion must not be null");
  }

  DispatchResult handle(JsonNode requestNode) {
    if (!(requestNode instanceof ObjectNode)) {
      ObjectNode error =
          errorResponse(
              JsonNodeFactory.instance.nullNode(),
              -32600,
              "Invalid Request",
              "Request payload must be a JSON object.");
      return new DispatchResult(error, false);
    }

    ObjectNode request = (ObjectNode) requestNode;
    JsonNode idNode = request.get("id");
    boolean isNotification = idNode == null || idNode.isNull();

    JsonNode methodNode = request.get("method");
    if (methodNode == null || !methodNode.isTextual()) {
      ObjectNode error =
          errorResponse(
              isNotification ? JsonNodeFactory.instance.nullNode() : idNode,
              -32600,
              "Invalid Request",
              "Missing or invalid 'method' field.");
      return new DispatchResult(isNotification ? null : error, false);
    }

    String method = methodNode.asText();

    if (method.startsWith("notifications/")) {
      return new DispatchResult(null, false);
    }

    try {
      switch (method) {
        case "initialize":
          return handleInitialize(idNode, isNotification, request.get("params"));
        case "shutdown":
          return handleShutdown(idNode, isNotification);
        case "ping":
          return handlePing(idNode, isNotification);
        case "tools/list":
          return handleToolsList(idNode, isNotification);
        case "tools/call":
          return handleToolsCall(request.get("params"), idNode, isNotification);
        case "prompts/list":
          return handlePromptsList(idNode, isNotification);
        case "resources/list":
          return handleResourcesList(idNode, isNotification);
        default:
          ObjectNode error =
              errorResponse(
                  isNotification ? JsonNodeFactory.instance.nullNode() : idNode,
                  -32601,
                  "Method not found",
                  "Unsupported method: " + method);
          return new DispatchResult(isNotification ? null : error, false);
      }
    } catch (IllegalArgumentException validationError) {
      ObjectNode error =
          errorResponse(
              isNotification ? JsonNodeFactory.instance.nullNode() : idNode,
              -32602,
              "Invalid params",
              validationError.getMessage());
      return new DispatchResult(isNotification ? null : error, false);
    } catch (Exception unexpected) {
      ObjectNode error =
          errorResponse(
              isNotification ? JsonNodeFactory.instance.nullNode() : idNode,
              -32603,
              "Internal error",
              Optional.ofNullable(unexpected.getMessage()).orElse(unexpected.toString()));
      return new DispatchResult(isNotification ? null : error, false);
    }
  }

  private DispatchResult handleInitialize(
      JsonNode idNode, boolean isNotification, JsonNode params) {
    String negotiatedProtocol = "0.1";
    if (params != null && params.hasNonNull("protocolVersion")) {
      negotiatedProtocol = params.get("protocolVersion").asText("0.1");
    }
    ObjectNode result = mapper.createObjectNode();
    result.put("protocolVersion", negotiatedProtocol);
    ObjectNode serverInfo = result.putObject("serverInfo");
    serverInfo.put("name", serverName);
    serverInfo.put("version", serverVersion);

    ObjectNode capabilities = result.putObject("capabilities");
    ObjectNode toolsCaps = capabilities.putObject("tools");
    toolsCaps.put("listChanged", false);
    capabilities.set("resources", mapper.createObjectNode());
    capabilities.set("prompts", mapper.createObjectNode());

    ObjectNode response = isNotification ? null : successResponse(idNode, result);
    return new DispatchResult(response, false);
  }

  private DispatchResult handleShutdown(JsonNode idNode, boolean isNotification) {
    ObjectNode response =
        isNotification ? null : successResponse(idNode, mapper.createObjectNode());
    return new DispatchResult(response, true);
  }

  private DispatchResult handlePing(JsonNode idNode, boolean isNotification) {
    ObjectNode pong = mapper.createObjectNode();
    pong.put("status", "ok");
    ObjectNode response = isNotification ? null : successResponse(idNode, pong);
    return new DispatchResult(response, false);
  }

  private DispatchResult handleToolsList(JsonNode idNode, boolean isNotification) {
    ObjectNode result = mapper.createObjectNode();
    ArrayNode toolArray = mapper.createArrayNode();
    for (McpTool tool : tools) {
      ObjectNode toolNode = mapper.createObjectNode();
      toolNode.put("name", tool.name());
      toolNode.put("description", tool.description());
      toolNode.set("inputSchema", tool.inputSchema(mapper));
      toolArray.add(toolNode);
    }
    result.set("tools", toolArray);
    ObjectNode response = isNotification ? null : successResponse(idNode, result);
    return new DispatchResult(response, false);
  }

  private DispatchResult handleToolsCall(JsonNode params, JsonNode idNode, boolean isNotification)
      throws Exception {
    if (!(params instanceof ObjectNode)) {
      throw new IllegalArgumentException("'params' must be an object for tools/call.");
    }
    ObjectNode parameters = (ObjectNode) params;
    JsonNode nameNode = parameters.get("name");
    if (nameNode == null || !nameNode.isTextual()) {
      throw new IllegalArgumentException("Tool name must be provided as a string.");
    }
    String toolName = nameNode.asText();
    McpTool tool =
        tools.stream()
            .filter(t -> t.name().equals(toolName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown tool: " + toolName));

    JsonNode arguments = parameters.get("arguments");
    if (arguments == null) {
      arguments = mapper.createObjectNode();
    }

    ToolExecutionResult execution = tool.call(mapper, arguments);
    ObjectNode result = mapper.createObjectNode();
    ArrayNode content = result.putArray("content");
    ObjectNode textBlock = mapper.createObjectNode();
    textBlock.put("type", "text");
    textBlock.put("text", execution.text());
    content.add(textBlock);
    result.put("isError", execution.isError());
    if (execution.metadata() != null) {
      result.set("meta", execution.metadata());
    }

    ObjectNode response = isNotification ? null : successResponse(idNode, result);
    return new DispatchResult(response, false);
  }

  private ObjectNode successResponse(JsonNode idNode, ObjectNode result) {
    ObjectNode response = mapper.createObjectNode();
    response.put("jsonrpc", JSON_RPC_VERSION);
    response.set("id", copyId(idNode));
    response.set("result", result);
    return response;
  }

  private DispatchResult handlePromptsList(JsonNode idNode, boolean isNotification) {
    ObjectNode result = mapper.createObjectNode();
    result.set("prompts", mapper.createArrayNode());
    ObjectNode response = isNotification ? null : successResponse(idNode, result);
    return new DispatchResult(response, false);
  }

  private DispatchResult handleResourcesList(JsonNode idNode, boolean isNotification) {
    ObjectNode result = mapper.createObjectNode();
    result.set("resources", mapper.createArrayNode());
    ObjectNode response = isNotification ? null : successResponse(idNode, result);
    return new DispatchResult(response, false);
  }

  private ObjectNode errorResponse(JsonNode idNode, int code, String message, String data) {
    ObjectNode response = mapper.createObjectNode();
    response.put("jsonrpc", JSON_RPC_VERSION);
    response.set("id", copyId(idNode));
    ObjectNode error = mapper.createObjectNode();
    error.put("code", code);
    error.put("message", message);
    if (data != null && !data.isEmpty()) {
      error.put("data", data);
    }
    response.set("error", error);
    return response;
  }

  private JsonNode copyId(JsonNode idNode) {
    if (idNode == null || idNode.isNull()) {
      return JsonNodeFactory.instance.nullNode();
    }
    return idNode.deepCopy();
  }

  static final class DispatchResult {
    private final ObjectNode response;
    private final boolean shouldExit;

    DispatchResult(ObjectNode response, boolean shouldExit) {
      this.response = response;
      this.shouldExit = shouldExit;
    }

    ObjectNode response() {
      return response;
    }

    boolean shouldExit() {
      return shouldExit;
    }
  }
}
