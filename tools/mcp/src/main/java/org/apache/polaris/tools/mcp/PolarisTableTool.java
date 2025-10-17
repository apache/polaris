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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/** MCP tool that exposes table-focused operations from the Polaris REST API. */
final class PolarisTableTool implements McpTool {
  private static final String TOOL_NAME = "polaris-table-request";
  private static final String TOOL_DESCRIPTION =
      "Perform table-centric operations (list, get, create, commit, delete) using the Polaris REST API.";
  private static final Set<String> LIST_ALIASES = Set.of("list", "ls");
  private static final Set<String> GET_ALIASES = Set.of("get", "load", "fetch");
  private static final Set<String> CREATE_ALIASES = Set.of("create");
  private static final Set<String> COMMIT_ALIASES = Set.of("commit", "update");
  private static final Set<String> DELETE_ALIASES = Set.of("delete", "drop");

  private final ObjectMapper mapper;
  private final PolarisRestTool delegate;

  PolarisTableTool(
      ObjectMapper mapper,
      HttpExecutor executor,
      URI baseUri,
      AuthorizationProvider authorizationProvider) {
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.delegate =
        new PolarisRestTool(
            "polaris.table.delegate",
            "Internal delegate for table operations",
            baseUri,
            "api/catalog/v1/",
            mapper,
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
    opEnum.add("commit");
    opEnum.add("delete");
    operation.put(
        "description",
        "Table operation to execute. "
            + "Supported values: list, get (synonyms: load, fetch), create, commit (synonym: update), delete (synonym: drop).");
    properties.set("operation", operation);

    ObjectNode catalog = mapper.createObjectNode();
    catalog.put("type", "string");
    catalog.put("description", "Polaris catalog identifier (maps to the {prefix} path segment).");
    properties.set("catalog", catalog);

    ObjectNode namespace = mapper.createObjectNode();
    ArrayNode namespaceAnyOf = namespace.putArray("anyOf");
    namespaceAnyOf.addObject().put("type", "string");
    ObjectNode namespaceArray = namespaceAnyOf.addObject();
    namespaceArray.put("type", "array");
    namespaceArray.set("items", mapper.createObjectNode().put("type", "string"));
    namespace.put(
        "description",
        "Namespace that contains the target tables. "
            + "Provide as a dot-delimited string (e.g. \"analytics.daily\") or an array of strings.");
    properties.set("namespace", namespace);

    ObjectNode table = mapper.createObjectNode();
    table.put("type", "string");
    table.put(
        "description",
        "Table identifier for operations that target a specific table (get, commit, delete).");
    properties.set("table", table);

    ObjectNode query = mapper.createObjectNode();
    query.put("type", "object");
    query.put(
        "description",
        "Optional query string parameters (for example page-size, page-token, include-drop).");
    query.putObject("additionalProperties").put("type", "string");
    properties.set("query", query);

    ObjectNode headers = mapper.createObjectNode();
    headers.put("type", "object");
    headers.put("description", "Optional additional HTTP headers to include with the request.");
    headers.putObject("additionalProperties").put("type", "string");
    properties.set("headers", headers);

    ObjectNode body = mapper.createObjectNode();
    body.put("description", "Optional request body payload for create or commit operations.");
    body.put("type", "object");
    properties.set("body", body);

    ArrayNode required = schema.putArray("required");
    required.add("operation");
    required.add("catalog");
    required.add("namespace");

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

    String catalog = encodeSegment(requireText(args, "catalog"));
    String namespace = encodeSegment(resolveNamespace(args.get("namespace")));

    ObjectNode delegateArgs = mapper.createObjectNode();
    copyIfObject(args.get("query"), delegateArgs, "query");
    copyIfObject(args.get("headers"), delegateArgs, "headers");

    switch (normalized) {
      case "list":
        handleList(args, delegateArgs, catalog, namespace);
        break;
      case "get":
        handleGet(args, delegateArgs, catalog, namespace);
        break;
      case "create":
        handleCreate(args, delegateArgs, catalog, namespace);
        break;
      case "commit":
        handleCommit(args, delegateArgs, catalog, namespace);
        break;
      case "delete":
        handleDelete(args, delegateArgs, catalog, namespace);
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }

    return delegate.call(mapper, delegateArgs);
  }

  private void handleList(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    String path = catalog + "/namespaces/" + namespace + "/tables";
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", path);
  }

  private void handleGet(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    String table =
        encodeSegment(requireText(args, "table", "Table name is required for get operations."));
    String path = catalog + "/namespaces/" + namespace + "/tables/" + table;
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", path);
  }

  private void handleCreate(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Create operations require a request body that matches the CreateTableRequest schema.");
    }
    String path = catalog + "/namespaces/" + namespace + "/tables";
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", path);
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleCommit(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Commit operations require a request body that matches the CommitTableRequest schema.");
    }
    String table =
        encodeSegment(requireText(args, "table", "Table name is required for commit operations."));
    String path = catalog + "/namespaces/" + namespace + "/tables/" + table;
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", path);
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleDelete(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    String table =
        encodeSegment(requireText(args, "table", "Table name is required for delete operations."));
    String path = catalog + "/namespaces/" + namespace + "/tables/" + table;
    delegateArgs.put("method", "DELETE");
    delegateArgs.put("path", path);
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
    if (COMMIT_ALIASES.contains(operation)) {
      return "commit";
    }
    if (DELETE_ALIASES.contains(operation)) {
      return "delete";
    }
    throw new IllegalArgumentException("Unsupported operation: " + operation);
  }

  private String resolveNamespace(JsonNode namespaceNode) {
    if (namespaceNode == null || namespaceNode.isNull()) {
      throw new IllegalArgumentException("Namespace must be provided.");
    }
    if (namespaceNode.isArray()) {
      ArrayNode array = (ArrayNode) namespaceNode;
      if (array.isEmpty()) {
        throw new IllegalArgumentException("Namespace array must contain at least one element.");
      }
      String[] parts = new String[array.size()];
      for (int i = 0; i < array.size(); i++) {
        JsonNode element = array.get(i);
        if (!element.isTextual() || element.asText().trim().isEmpty()) {
          throw new IllegalArgumentException("Namespace array elements must be non-empty strings.");
        }
        parts[i] = element.asText().trim();
      }
      return String.join(".", parts);
    }
    if (!namespaceNode.isTextual() || namespaceNode.asText().trim().isEmpty()) {
      throw new IllegalArgumentException("Namespace must be a non-empty string.");
    }
    return namespaceNode.asText().trim();
  }

  private static void copyIfObject(JsonNode source, ObjectNode target, String fieldName) {
    if (source instanceof ObjectNode) {
      target.set(fieldName, ((ObjectNode) source).deepCopy());
    }
  }

  private static String encodeSegment(String value) {
    String encoded = URLEncoder.encode(value, StandardCharsets.UTF_8);
    return encoded.replace("+", "%20");
  }

  private static String requireText(ObjectNode node, String field) {
    return requireText(node, field, "Missing required field: " + field);
  }

  private static String requireText(ObjectNode node, String field, String errorMessage) {
    JsonNode value = node.get(field);
    if (value == null || !value.isTextual() || value.asText().trim().isEmpty()) {
      throw new IllegalArgumentException(errorMessage);
    }
    return value.asText().trim();
  }
}
