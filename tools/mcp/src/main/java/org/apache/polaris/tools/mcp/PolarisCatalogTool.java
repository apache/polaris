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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/** MCP tool for Polaris management catalog operations. */
final class PolarisCatalogTool implements McpTool {
  private static final String TOOL_NAME = "polaris-catalog-request";
  private static final String TOOL_DESCRIPTION =
      "Interact with the Polaris management API for catalog lifecycle operations.";

  private static final Set<String> LIST_ALIASES = Set.of("list");
  private static final Set<String> GET_ALIASES = Set.of("get");
  private static final Set<String> CREATE_ALIASES = Set.of("create");
  private static final Set<String> UPDATE_ALIASES = Set.of("update");
  private static final Set<String> DELETE_ALIASES = Set.of("delete", "drop", "remove");

  private final PolarisRestTool delegate;

  PolarisCatalogTool(
      ObjectMapper mapper,
      HttpExecutor executor,
      URI baseUri,
      AuthorizationProvider authorizationProvider) {
    this.delegate =
        new PolarisRestTool(
            "polaris.catalog.delegate",
            "Internal delegate for catalog operations",
            baseUri,
            "api/management/v1/",
            Objects.requireNonNull(mapper, "mapper must not be null"),
            Objects.requireNonNull(executor, "executor must not be null"),
            Objects.requireNonNull(
                authorizationProvider, "authorizationProvider must not be null"));
  }

  @Override
  public String name() {
    return TOOL_NAME;
  }

  @Override
  public String description() {
    return TOOL_DESCRIPTION;
  }

  @Override
  public ObjectNode inputSchema(ObjectMapper mapper) {
    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "object");

    ObjectNode properties = schema.putObject("properties");

    ObjectNode operation = mapper.createObjectNode();
    operation.put("type", "string");
    ArrayNode opEnum = operation.putArray("enum");
    opEnum.add("list");
    opEnum.add("get");
    opEnum.add("create");
    opEnum.add("update");
    opEnum.add("delete");
    operation.put(
        "description",
        "Catalog operation to execute. Supported values: list, get, create, update, delete.");
    properties.set("operation", operation);

    ObjectNode catalog = mapper.createObjectNode();
    catalog.put("type", "string");
    catalog.put(
        "description",
        "Catalog name (required for get, update, delete). Automatically appended to the path.");
    properties.set("catalog", catalog);

    ObjectNode query = mapper.createObjectNode();
    query.put("type", "object");
    query.put("description", "Optional query parameters.");
    query.putObject("additionalProperties").put("type", "string");
    properties.set("query", query);

    ObjectNode headers = mapper.createObjectNode();
    headers.put("type", "object");
    headers.put("description", "Optional request headers.");
    headers.putObject("additionalProperties").put("type", "string");
    properties.set("headers", headers);

    ObjectNode body = mapper.createObjectNode();
    body.put(
        "description",
        "Optional request body payload for create/update. See polaris-management-service.yml.");
    body.put("type", "object");
    properties.set("body", body);

    ArrayNode required = schema.putArray("required");
    required.add("operation");

    return schema;
  }

  @Override
  public ToolExecutionResult call(ObjectMapper mapper, JsonNode arguments) throws Exception {
    if (!(arguments instanceof ObjectNode)) {
      throw new IllegalArgumentException("Tool arguments must be a JSON object.");
    }
    ObjectNode args = (ObjectNode) arguments;

    String operation = requireText(args, "operation").toLowerCase(Locale.ROOT).trim();
    String normalized = normalizeOperation(operation);

    ObjectNode delegateArgs = mapper.createObjectNode();
    copyIfObject(args.get("query"), delegateArgs, "query");
    copyIfObject(args.get("headers"), delegateArgs, "headers");

    switch (normalized) {
      case "list":
        handleList(delegateArgs);
        break;
      case "get":
        handleGet(args, delegateArgs);
        break;
      case "create":
        handleCreate(args, delegateArgs);
        break;
      case "update":
        handleUpdate(args, delegateArgs);
        break;
      case "delete":
        handleDelete(args, delegateArgs);
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }

    ToolExecutionResult raw = delegate.call(mapper, delegateArgs);
    return maybeAugmentError(raw, normalized, mapper);
  }

  private void handleList(ObjectNode delegateArgs) {
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", "catalogs");
  }

  private void handleGet(ObjectNode args, ObjectNode delegateArgs) {
    String catalogName = requireText(args, "catalog");
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", "catalogs/" + catalogName);
  }

  private void handleCreate(ObjectNode args, ObjectNode delegateArgs) {
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Create operations require a body matching CreateCatalogRequest.");
    }
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", "catalogs");
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleUpdate(ObjectNode args, ObjectNode delegateArgs) {
    String catalogName = requireText(args, "catalog");
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Update operations require a body matching UpdateCatalogRequest.");
    }
    delegateArgs.put("method", "PUT");
    delegateArgs.put("path", "catalogs/" + catalogName);
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleDelete(ObjectNode args, ObjectNode delegateArgs) {
    String catalogName = requireText(args, "catalog");
    delegateArgs.put("method", "DELETE");
    delegateArgs.put("path", "catalogs/" + catalogName);
  }

  private ToolExecutionResult maybeAugmentError(
      ToolExecutionResult result, String operation, ObjectMapper mapper) {
    if (!result.isError()) {
      return result;
    }
    ObjectNode metadata = result.metadata();
    if (metadata == null) {
      metadata = mapper.createObjectNode();
    }
    int status = metadata.path("response").path("status").asInt(-1);
    if (status != 400 && status != 409) {
      return result;
    }

    String hint = null;
    switch (operation) {
      case "create":
        hint =
            "Create requests must include catalog configuration in the body. "
                + "See CreateCatalogRequest in spec/polaris-management-service.yml.";
        break;
      case "update":
        hint =
            "Update requests require the catalog name in the path and body matching UpdateCatalogRequest. "
                + "Ensure currentEntityVersion matches the latest catalog version.";
        break;
      default:
        break;
    }
    if (hint == null) {
      return result;
    }
    metadata.put("hint", hint);
    String text = result.text();
    if (!text.contains(hint)) {
      text = text + System.lineSeparator() + "Hint: " + hint;
    }
    return new ToolExecutionResult(text, true, metadata);
  }

  private static void copyIfObject(JsonNode source, ObjectNode target, String fieldName) {
    if (source instanceof ObjectNode) {
      target.set(fieldName, ((ObjectNode) source).deepCopy());
    }
  }

  private static String normalizeOperation(String operation) {
    if (LIST_ALIASES.contains(operation)) {
      return "list";
    }
    if (GET_ALIASES.contains(operation)) {
      return "get";
    }
    if (CREATE_ALIASES.contains(operation)) {
      return "create";
    }
    if (UPDATE_ALIASES.contains(operation)) {
      return "update";
    }
    if (DELETE_ALIASES.contains(operation)) {
      return "delete";
    }
    throw new IllegalArgumentException("Unsupported operation: " + operation);
  }

  private static String requireText(ObjectNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || !value.isTextual() || value.asText().trim().isEmpty()) {
      throw new IllegalArgumentException("Missing required field: " + field);
    }
    return value.asText();
  }
}
