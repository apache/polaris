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

/** MCP tool exposing Polaris policy management endpoints. */
final class PolarisPolicyTool implements McpTool {
  private static final String TOOL_NAME = "polaris-policy-request";
  private static final String TOOL_DESCRIPTION =
      "Manage Polaris policies (list, create, update, delete, attach, detach, applicable).";

  private static final Set<String> LIST_ALIASES = Set.of("list");
  private static final Set<String> GET_ALIASES = Set.of("get", "load", "fetch");
  private static final Set<String> CREATE_ALIASES = Set.of("create");
  private static final Set<String> UPDATE_ALIASES = Set.of("update");
  private static final Set<String> DELETE_ALIASES = Set.of("delete", "drop", "remove");
  private static final Set<String> ATTACH_ALIASES = Set.of("attach", "map");
  private static final Set<String> DETACH_ALIASES = Set.of("detach", "unmap", "unattach");
  private static final Set<String> APPLICABLE_ALIASES = Set.of("applicable", "applicable-policies");

  private final ObjectMapper mapper;
  private final PolarisRestTool delegate;

  PolarisPolicyTool(
      ObjectMapper mapper,
      HttpExecutor executor,
      URI baseUri,
      AuthorizationProvider authorizationProvider) {
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
    this.delegate =
        new PolarisRestTool(
            "polaris.policy.delegate",
            "Internal delegate for policy operations",
            baseUri,
            "api/catalog/polaris/v1/",
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
    opEnum.add("update");
    opEnum.add("delete");
    opEnum.add("attach");
    opEnum.add("detach");
    opEnum.add("applicable");
    operation.put(
        "description",
        "Policy operation to execute. Supported values: list, get, create, update, delete, attach, detach, applicable.");
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
        "Namespace that contains the target policies. Provide as a dot-delimited string (e.g. \"analytics.daily\") or an array of strings.");
    properties.set("namespace", namespace);

    ObjectNode policy = mapper.createObjectNode();
    policy.put("type", "string");
    policy.put("description", "Policy identifier for operations that target a specific policy.");
    properties.set("policy", policy);

    ObjectNode query = mapper.createObjectNode();
    query.put("type", "object");
    query.put(
        "description",
        "Optional query string parameters (for example page-size, policy-type, detach-all).");
    query.putObject("additionalProperties").put("type", "string");
    properties.set("query", query);

    ObjectNode headers = mapper.createObjectNode();
    headers.put("type", "object");
    headers.put("description", "Optional additional HTTP headers to include with the request.");
    headers.putObject("additionalProperties").put("type", "string");
    properties.set("headers", headers);

    ObjectNode body = mapper.createObjectNode();
    body.put(
        "description",
        "Optional request body payload for create/update/attach/detach operations. The structure must follow the corresponding Polaris REST schema.");
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
    String namespace = null;
    if (!"applicable".equals(normalized)) {
      namespace = encodeSegment(resolveNamespace(args.get("namespace")));
    } else if (args.hasNonNull("namespace")) {
      namespace = encodeSegment(resolveNamespace(args.get("namespace")));
    }

    ObjectNode delegateArgs = mapper.createObjectNode();
    copyIfObject(args.get("query"), delegateArgs, "query");
    copyIfObject(args.get("headers"), delegateArgs, "headers");

    switch (normalized) {
      case "list":
        requireNamespace(namespace, "list");
        handleList(delegateArgs, catalog, namespace);
        break;
      case "get":
        requireNamespace(namespace, "get");
        handleGet(args, delegateArgs, catalog, namespace);
        break;
      case "create":
        requireNamespace(namespace, "create");
        handleCreate(args, delegateArgs, catalog, namespace);
        break;
      case "update":
        requireNamespace(namespace, "update");
        handleUpdate(args, delegateArgs, catalog, namespace);
        break;
      case "delete":
        requireNamespace(namespace, "delete");
        handleDelete(args, delegateArgs, catalog, namespace);
        break;
      case "attach":
        requireNamespace(namespace, "attach");
        handleAttach(args, delegateArgs, catalog, namespace);
        break;
      case "detach":
        requireNamespace(namespace, "detach");
        handleDetach(args, delegateArgs, catalog, namespace);
        break;
      case "applicable":
        handleApplicable(delegateArgs, catalog);
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }

    ToolExecutionResult rawResult = delegate.call(mapper, delegateArgs);
    return maybeAugmentError(rawResult, normalized, mapper);
  }

  private void handleList(ObjectNode delegateArgs, String catalog, String namespace) {
    String path = catalog + "/namespaces/" + namespace + "/policies";
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", path);
  }

  private void handleGet(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    String policy =
        encodeSegment(requireText(args, "policy", "Policy name is required for get operations."));
    String path = catalog + "/namespaces/" + namespace + "/policies/" + policy;
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", path);
  }

  private void handleCreate(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Create operations require a request body that matches the CreatePolicyRequest schema.");
    }
    String path = catalog + "/namespaces/" + namespace + "/policies";
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", path);
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleUpdate(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Update operations require a request body that matches the UpdatePolicyRequest schema.");
    }
    String policy =
        encodeSegment(
            requireText(args, "policy", "Policy name is required for update operations."));
    String path = catalog + "/namespaces/" + namespace + "/policies/" + policy;
    delegateArgs.put("method", "PUT");
    delegateArgs.put("path", path);
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleDelete(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    String policy =
        encodeSegment(
            requireText(args, "policy", "Policy name is required for delete operations."));
    String path = catalog + "/namespaces/" + namespace + "/policies/" + policy;
    delegateArgs.put("method", "DELETE");
    delegateArgs.put("path", path);
  }

  private void handleAttach(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Attach operations require a request body that matches the AttachPolicyRequest schema.");
    }
    String policy =
        encodeSegment(
            requireText(args, "policy", "Policy name is required for attach operations."));
    String path = catalog + "/namespaces/" + namespace + "/policies/" + policy + "/mappings";
    delegateArgs.put("method", "PUT");
    delegateArgs.put("path", path);
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleDetach(
      ObjectNode args, ObjectNode delegateArgs, String catalog, String namespace) {
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Detach operations require a request body that matches the DetachPolicyRequest schema.");
    }
    String policy =
        encodeSegment(
            requireText(args, "policy", "Policy name is required for detach operations."));
    String path = catalog + "/namespaces/" + namespace + "/policies/" + policy + "/mappings";
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", path);
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleApplicable(ObjectNode delegateArgs, String catalog) {
    String path = catalog + "/applicable-policies";
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", path);
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
            "Create requests must include `name`, `type`, and optional `description`/`content` in the body. "
                + "See CreatePolicyRequest in spec/polaris-catalog-apis/policy-apis.yaml. "
                + "Common types include system.data-compaction, system.metadata-compaction, "
                + "system.orphan-file-removal, and system.snapshot-expiry. "
                + "Example: {\"name\":\"weekly_compaction\",\"type\":\"system.data-compaction\",\"content\":{...}}. "
                + "Reference schema: http://polaris.apache.org/schemas/policies/system/data-compaction/2025-02-03.json";
        break;
      case "update":
        hint =
            "Update requests require the policy name in the path and the body with `description`, `content`, and `currentVersion`.";
        break;
      case "attach":
        hint =
            "Attach requests require a body with `targetType`, `targetName`, and optional `parameters`.";
        break;
      case "detach":
        hint =
            "Detach requests require a body with `targetType`, `targetName`, and optional `parameters`.";
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
    if (ATTACH_ALIASES.contains(operation)) {
      return "attach";
    }
    if (DETACH_ALIASES.contains(operation)) {
      return "detach";
    }
    if (APPLICABLE_ALIASES.contains(operation)) {
      return "applicable";
    }
    throw new IllegalArgumentException("Unsupported operation: " + operation);
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

  private static void requireNamespace(String namespace, String operation) {
    if (namespace == null || namespace.isEmpty()) {
      throw new IllegalArgumentException(
          "Namespace is required for "
              + operation
              + " operations. Provide `namespace` as a string or array.");
    }
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
}
