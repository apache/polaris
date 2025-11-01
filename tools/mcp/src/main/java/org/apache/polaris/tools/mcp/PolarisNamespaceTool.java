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

/** MCP tool for namespace lifecycle operations. */
final class PolarisNamespaceTool implements McpTool {
  private static final String TOOL_NAME = "polaris-namespace-request";
  private static final String TOOL_DESCRIPTION =
      "Manage namespaces in an Iceberg catalog (list, get, create, update properties, delete).";

  private static final Set<String> LIST_ALIASES = Set.of("list");
  private static final Set<String> GET_ALIASES = Set.of("get", "load");
  private static final Set<String> CREATE_ALIASES = Set.of("create");
  private static final Set<String> DELETE_ALIASES = Set.of("delete", "drop", "remove");
  private static final Set<String> UPDATE_PROPS_ALIASES =
      Set.of("update-properties", "set-properties", "properties-update");
  private static final Set<String> GET_PROPS_ALIASES = Set.of("get-properties", "properties");
  private static final Set<String> EXISTS_ALIASES = Set.of("exists", "head");

  private final ObjectMapper mapper;
  private final PolarisRestTool delegate;

  PolarisNamespaceTool(
      ObjectMapper mapper,
      HttpExecutor executor,
      URI baseUri,
      AuthorizationProvider authorizationProvider) {
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.delegate =
        new PolarisRestTool(
            "polaris.namespace.delegate",
            "Internal delegate for namespace operations",
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
    opEnum.add("exists");
    opEnum.add("create");
    opEnum.add("update-properties");
    opEnum.add("get-properties");
    opEnum.add("delete");
    operation.put(
        "description",
        "Namespace operation to execute. Supported values: list, get, exists, create, "
            + "update-properties, get-properties, delete.");
    properties.set("operation", operation);

    ObjectNode catalog = mapper.createObjectNode();
    catalog.put("type", "string");
    catalog.put("description", "Catalog identifier (maps to the {prefix} path segment).");
    properties.set("catalog", catalog);

    ObjectNode namespace = mapper.createObjectNode();
    ArrayNode nsAnyOf = namespace.putArray("anyOf");
    nsAnyOf.addObject().put("type", "string");
    ObjectNode nsArray = nsAnyOf.addObject();
    nsArray.put("type", "array");
    nsArray.set("items", mapper.createObjectNode().put("type", "string"));
    namespace.put(
        "description",
        "Namespace identifier. Provide as dot-delimited string (e.g. \"analytics.daily\") "
            + "or array of path components.");
    properties.set("namespace", namespace);

    ObjectNode query = mapper.createObjectNode();
    query.put("type", "object");
    query.put(
        "description", "Optional query string parameters (for example page-size, page-token).");
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
        "Optional request body payload (required for create and update-properties). "
            + "See the Iceberg REST catalog specification for the expected schema.");
    body.put("type", "object");
    properties.set("body", body);

    ArrayNode required = schema.putArray("required");
    required.add("operation");
    required.add("catalog");

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
    ObjectNode delegateArgs = mapper.createObjectNode();
    copyIfObject(args.get("query"), delegateArgs, "query");
    copyIfObject(args.get("headers"), delegateArgs, "headers");

    switch (normalized) {
      case "list":
        handleList(delegateArgs, catalog);
        break;
      case "get":
        handleGet(args, delegateArgs, catalog);
        break;
      case "exists":
        handleExists(args, delegateArgs, catalog);
        break;
      case "create":
        handleCreate(args, delegateArgs, catalog);
        break;
      case "update-properties":
        handleUpdateProperties(args, delegateArgs, catalog);
        break;
      case "get-properties":
        handleGetProperties(args, delegateArgs, catalog);
        break;
      case "delete":
        handleDelete(args, delegateArgs, catalog);
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }

    ToolExecutionResult raw = delegate.call(mapper, delegateArgs);
    return maybeAugmentError(raw, normalized, mapper);
  }

  private void handleList(ObjectNode delegateArgs, String catalog) {
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", catalog + "/namespaces");
  }

  private void handleGet(ObjectNode args, ObjectNode delegateArgs, String catalog) {
    String namespace = resolveNamespacePath(args);
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", catalog + "/namespaces/" + namespace);
  }

  private void handleExists(ObjectNode args, ObjectNode delegateArgs, String catalog) {
    String namespace = resolveNamespacePath(args);
    delegateArgs.put("method", "HEAD");
    delegateArgs.put("path", catalog + "/namespaces/" + namespace);
  }

  private void handleCreate(ObjectNode args, ObjectNode delegateArgs, String catalog) {
    JsonNode body = args.get("body");
    ObjectNode bodyObj = body instanceof ObjectNode ? (ObjectNode) body : mapper.createObjectNode();

    if (!bodyObj.hasNonNull("namespace")) {
      // if namespace argument provided, add to body
      if (args.hasNonNull("namespace")) {
        ArrayNode nsArray = mapper.createArrayNode();
        for (String part : resolveNamespaceArray(args)) {
          nsArray.add(part);
        }
        bodyObj.set("namespace", nsArray);
      } else {
        throw new IllegalArgumentException(
            "Create operations require `body.namespace` or the `namespace` argument.");
      }
    }

    delegateArgs.put("method", "POST");
    delegateArgs.put("path", catalog + "/namespaces");
    delegateArgs.set("body", bodyObj);
  }

  private void handleUpdateProperties(ObjectNode args, ObjectNode delegateArgs, String catalog) {
    String namespace = resolveNamespacePath(args);
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "update-properties requires a body matching UpdateNamespacePropertiesRequest.");
    }
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", catalog + "/namespaces/" + namespace + "/properties");
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleGetProperties(ObjectNode args, ObjectNode delegateArgs, String catalog) {
    String namespace = resolveNamespacePath(args);
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", catalog + "/namespaces/" + namespace + "/properties");
  }

  private void handleDelete(ObjectNode args, ObjectNode delegateArgs, String catalog) {
    String namespace = resolveNamespacePath(args);
    delegateArgs.put("method", "DELETE");
    delegateArgs.put("path", catalog + "/namespaces/" + namespace);
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
    if (status != 400 && status != 422) {
      return result;
    }

    String hint = null;
    switch (operation) {
      case "create":
        hint =
            "Create requests must include `namespace` (array of strings) in the body and optional `properties`. "
                + "See CreateNamespaceRequest in spec/iceberg-rest-catalog-open-api.yaml.";
        break;
      case "update-properties":
        hint =
            "update-properties requests require `body` with `updates` and/or `removals`. "
                + "See UpdateNamespacePropertiesRequest in spec/iceberg-rest-catalog-open-api.yaml.";
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

  private static String[] resolveNamespaceArray(ObjectNode args) {
    JsonNode namespaceNode = args.get("namespace");
    if (namespaceNode == null || namespaceNode.isNull()) {
      throw new IllegalArgumentException("Namespace must be provided.");
    }
    if (namespaceNode.isArray()) {
      ArrayNode array = (ArrayNode) namespaceNode;
      if (array.isEmpty()) {
        throw new IllegalArgumentException("Namespace array must contain at least one component.");
      }
      String[] parts = new String[array.size()];
      for (int i = 0; i < array.size(); i++) {
        JsonNode element = array.get(i);
        if (!element.isTextual() || element.asText().trim().isEmpty()) {
          throw new IllegalArgumentException("Namespace array elements must be non-empty strings.");
        }
        parts[i] = element.asText().trim();
      }
      return parts;
    }
    if (!namespaceNode.isTextual() || namespaceNode.asText().trim().isEmpty()) {
      throw new IllegalArgumentException("Namespace must be a non-empty string.");
    }
    return namespaceNode.asText().trim().split("\\.");
  }

  private static String resolveNamespacePath(ObjectNode args) {
    String[] parts = resolveNamespaceArray(args);
    String joined = String.join(".", parts);
    return encodeSegment(joined);
  }

  private static String encodeSegment(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
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
    if (EXISTS_ALIASES.contains(operation)) {
      return "exists";
    }
    if (CREATE_ALIASES.contains(operation)) {
      return "create";
    }
    if (UPDATE_PROPS_ALIASES.contains(operation)) {
      return "update-properties";
    }
    if (GET_PROPS_ALIASES.contains(operation)) {
      return "get-properties";
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
