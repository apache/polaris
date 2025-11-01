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

/** MCP tool for principal CRUD and role assignment operations. */
final class PolarisPrincipalTool implements McpTool {
  private static final String TOOL_NAME = "polaris-principal-request";
  private static final String TOOL_DESCRIPTION =
      "Manage principals via the Polaris management API (list, get, create, update, delete, rotate/reset credentials, role assignment).";

  private static final Set<String> LIST_ALIASES = Set.of("list");
  private static final Set<String> CREATE_ALIASES = Set.of("create");
  private static final Set<String> GET_ALIASES = Set.of("get");
  private static final Set<String> UPDATE_ALIASES = Set.of("update");
  private static final Set<String> DELETE_ALIASES = Set.of("delete", "remove");
  private static final Set<String> ROTATE_ALIASES = Set.of("rotate-credentials", "rotate");
  private static final Set<String> RESET_ALIASES = Set.of("reset-credentials", "reset");
  private static final Set<String> LIST_ROLES_ALIASES =
      Set.of("list-principal-roles", "list-roles");
  private static final Set<String> ASSIGN_ROLE_ALIASES =
      Set.of("assign-principal-role", "assign-role");
  private static final Set<String> REVOKE_ROLE_ALIASES =
      Set.of("revoke-principal-role", "revoke-role");

  private final PolarisRestTool delegate;

  PolarisPrincipalTool(
      ObjectMapper mapper,
      HttpExecutor executor,
      URI baseUri,
      AuthorizationProvider authorizationProvider) {
    this.delegate =
        new PolarisRestTool(
            "polaris.principal.delegate",
            "Internal delegate for principal operations",
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
    opEnum.add("create");
    opEnum.add("get");
    opEnum.add("update");
    opEnum.add("delete");
    opEnum.add("rotate-credentials");
    opEnum.add("reset-credentials");
    opEnum.add("list-principal-roles");
    opEnum.add("assign-principal-role");
    opEnum.add("revoke-principal-role");
    operation.put(
        "description",
        "Principal operation to execute (list, get, create, update, delete, rotate-credentials, "
            + "reset-credentials, list-principal-roles, assign-principal-role, revoke-principal-role)."
            + " Optional query parameters (e.g. catalog context) can be supplied via `query`.");
    properties.set("operation", operation);

    ObjectNode principal = mapper.createObjectNode();
    principal.put("type", "string");
    principal.put("description", "Principal name for operations targeting a specific principal.");
    properties.set("principal", principal);

    ObjectNode principalRole = mapper.createObjectNode();
    principalRole.put("type", "string");
    principalRole.put("description", "Principal role name for assignment/revocation operations.");
    properties.set("principalRole", principalRole);

    ObjectNode query = mapper.createObjectNode();
    query.put("type", "object");
    query.put("description", "Optional query string parameters.");
    query.putObject("additionalProperties").put("type", "string");
    properties.set("query", query);

    ObjectNode headers = mapper.createObjectNode();
    headers.put("type", "object");
    headers.put("description", "Optional request headers.");
    headers.putObject("additionalProperties").put("type", "string");
    properties.set("headers", headers);

    ObjectNode body = mapper.createObjectNode();
    body.put("type", mapper.createArrayNode().add("object").add("null"));
    body.put(
        "description",
        "Optional request body payload. Required for create/update, grant/revoke operations."
            + "See polaris-management-service.yml for schemas such as CreatePrincipalRequest, UpdatePrincipalRequest, GrantPrincipalRoleRequest.");
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
      case "create":
        handleCreate(args, delegateArgs);
        break;
      case "get":
        handleGet(args, delegateArgs);
        break;
      case "update":
        handleUpdate(args, delegateArgs);
        break;
      case "delete":
        handleDelete(args, delegateArgs);
        break;
      case "rotate-credentials":
        handleRotate(args, delegateArgs);
        break;
      case "reset-credentials":
        handleReset(args, delegateArgs);
        break;
      case "list-principal-roles":
        handleListRoles(args, delegateArgs);
        break;
      case "assign-principal-role":
        handleAssignRole(args, delegateArgs);
        break;
      case "revoke-principal-role":
        handleRevokeRole(args, delegateArgs);
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }

    ToolExecutionResult raw = delegate.call(mapper, delegateArgs);
    return maybeAugmentError(raw, normalized, mapper);
  }

  private void handleList(ObjectNode delegateArgs) {
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", "principals");
  }

  private void handleCreate(ObjectNode args, ObjectNode delegateArgs) {
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Create principal requires a body matching CreatePrincipalRequest.");
    }
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", "principals");
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleGet(ObjectNode args, ObjectNode delegateArgs) {
    String principal = requireText(args, "principal");
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", "principals/" + principal);
  }

  private void handleUpdate(ObjectNode args, ObjectNode delegateArgs) {
    String principal = requireText(args, "principal");
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "Update principal requires a body matching UpdatePrincipalRequest.");
    }
    delegateArgs.put("method", "PUT");
    delegateArgs.put("path", "principals/" + principal);
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleDelete(ObjectNode args, ObjectNode delegateArgs) {
    String principal = requireText(args, "principal");
    delegateArgs.put("method", "DELETE");
    delegateArgs.put("path", "principals/" + principal);
  }

  private void handleRotate(ObjectNode args, ObjectNode delegateArgs) {
    String principal = requireText(args, "principal");
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", "principals/" + principal + "/rotate");
  }

  private void handleReset(ObjectNode args, ObjectNode delegateArgs) {
    String principal = requireText(args, "principal");
    delegateArgs.put("method", "POST");
    delegateArgs.put("path", "principals/" + principal + "/reset");
    if (args.get("body") instanceof ObjectNode) {
      delegateArgs.set("body", args.get("body").deepCopy());
    }
  }

  private void handleListRoles(ObjectNode args, ObjectNode delegateArgs) {
    String principal = requireText(args, "principal");
    delegateArgs.put("method", "GET");
    delegateArgs.put("path", "principals/" + principal + "/principal-roles");
  }

  private void handleAssignRole(ObjectNode args, ObjectNode delegateArgs) {
    String principal = requireText(args, "principal");
    JsonNode body = args.get("body");
    if (!(body instanceof ObjectNode)) {
      throw new IllegalArgumentException(
          "assign-principal-role requires a body matching GrantPrincipalRoleRequest.");
    }
    delegateArgs.put("method", "PUT");
    delegateArgs.put("path", "principals/" + principal + "/principal-roles");
    delegateArgs.set("body", body.deepCopy());
  }

  private void handleRevokeRole(ObjectNode args, ObjectNode delegateArgs) {
    String principal = requireText(args, "principal");
    String role = requireText(args, "principalRole");
    delegateArgs.put("method", "DELETE");
    delegateArgs.put("path", "principals/" + principal + "/principal-roles/" + role);
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
            "Create principal requires a body matching CreatePrincipalRequest."
                + " See spec/polaris-management-service.yml.";
        break;
      case "update":
        hint =
            "Update principal requires `principal` and body matching UpdatePrincipalRequest with currentEntityVersion.";
        break;
      case "assign-principal-role":
        hint =
            "Provide GrantPrincipalRoleRequest in the body (principalRoleName, catalogName, etc.).";
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
    if (CREATE_ALIASES.contains(operation)) {
      return "create";
    }
    if (GET_ALIASES.contains(operation)) {
      return "get";
    }
    if (UPDATE_ALIASES.contains(operation)) {
      return "update";
    }
    if (DELETE_ALIASES.contains(operation)) {
      return "delete";
    }
    if (ROTATE_ALIASES.contains(operation)) {
      return "rotate-credentials";
    }
    if (RESET_ALIASES.contains(operation)) {
      return "reset-credentials";
    }
    if (LIST_ROLES_ALIASES.contains(operation)) {
      return "list-principal-roles";
    }
    if (ASSIGN_ROLE_ALIASES.contains(operation)) {
      return "assign-principal-role";
    }
    if (REVOKE_ROLE_ALIASES.contains(operation)) {
      return "revoke-principal-role";
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
