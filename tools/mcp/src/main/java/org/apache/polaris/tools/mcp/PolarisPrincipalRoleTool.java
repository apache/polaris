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

/** MCP tool for principal role operations. */
final class PolarisPrincipalRoleTool implements McpTool {
  private static final String TOOL_NAME = "polaris-principal-role-request";
  private static final String TOOL_DESCRIPTION =
      "Manage principal roles (list, get, create, update, delete) and their catalog-role assignments via the Polaris management API.";

  private static final Set<String> LIST_ALIASES = Set.of("list");
  private static final Set<String> CREATE_ALIASES = Set.of("create");
  private static final Set<String> GET_ALIASES = Set.of("get");
  private static final Set<String> UPDATE_ALIASES = Set.of("update");
  private static final Set<String> DELETE_ALIASES = Set.of("delete", "remove");
  private static final Set<String> LIST_PRINCIPALS_ALIASES =
      Set.of("list-principals", "list-assignees");
  private static final Set<String> LIST_CATALOG_ROLES_ALIASES =
      Set.of("list-catalog-roles", "list-mapped-catalog-roles");
  private static final Set<String> ASSIGN_CATALOG_ROLE_ALIASES =
      Set.of("assign-catalog-role", "grant-catalog-role");
  private static final Set<String> REVOKE_CATALOG_ROLE_ALIASES =
      Set.of("revoke-catalog-role", "remove-catalog-role");

  private final PolarisRestTool delegate;

  PolarisPrincipalRoleTool(
      ObjectMapper mapper,
      HttpExecutor executor,
      URI baseUri,
      AuthorizationProvider authorizationProvider) {
    this.delegate =
        new PolarisRestTool(
            "polaris.principalrole.delegate",
            "Internal delegate for principal role operations",
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
    opEnum.add("list-principals");
    opEnum.add("list-catalog-roles");
    opEnum.add("assign-catalog-role");
    opEnum.add("revoke-catalog-role");
    operation.put(
        "description",
        "Principal role operation to execute. Provide optional `catalog`/`catalogRole` when required (e.g. assignment).");
    properties.set("operation", operation);

    ObjectNode role = mapper.createObjectNode();
    role.put("type", "string");
    role.put("description", "Principal role name (required for role-specific operations).");
    properties.set("principalRole", role);

    ObjectNode catalog = mapper.createObjectNode();
    catalog.put("type", "string");
    catalog.put("description", "Catalog name for mapping operations to catalog roles.");
    properties.set("catalog", catalog);

    ObjectNode catalogRole = mapper.createObjectNode();
    catalogRole.put("type", "string");
    catalogRole.put("description", "Catalog role name for revoke operations.");
    properties.set("catalogRole", catalogRole);

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
    body.put(
        "description",
        "Optional request body payload (required for create/update and assign operations)."
            + " See polaris-management-service.yml for request schemas.");
    body.put("type", mapper.createArrayNode().add("object").add("null"));
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
        delegateArgs.put("method", "GET");
        delegateArgs.put("path", "principal-roles");
        break;
      case "create":
        delegateArgs.put("method", "POST");
        delegateArgs.put("path", "principal-roles");
        delegateArgs.set("body", requireObject(args, "body", "CreatePrincipalRoleRequest"));
        break;
      case "get":
        delegateArgs.put("method", "GET");
        delegateArgs.put("path", principalRolePath(args));
        break;
      case "update":
        delegateArgs.put("method", "PUT");
        delegateArgs.put("path", principalRolePath(args));
        delegateArgs.set("body", requireObject(args, "body", "UpdatePrincipalRoleRequest"));
        break;
      case "delete":
        delegateArgs.put("method", "DELETE");
        delegateArgs.put("path", principalRolePath(args));
        break;
      case "list-principals":
        delegateArgs.put("method", "GET");
        delegateArgs.put("path", principalRolePath(args) + "/principals");
        break;
      case "list-catalog-roles":
        delegateArgs.put("method", "GET");
        delegateArgs.put("path", principalRoleCatalogPath(args));
        break;
      case "assign-catalog-role":
        delegateArgs.put("method", "PUT");
        delegateArgs.put("path", principalRoleCatalogPath(args));
        delegateArgs.set("body", requireObject(args, "body", "GrantCatalogRoleRequest"));
        break;
      case "revoke-catalog-role":
        delegateArgs.put("method", "DELETE");
        delegateArgs.put(
            "path", principalRoleCatalogPath(args) + "/" + requireText(args, "catalogRole"));
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }

    ToolExecutionResult raw = delegate.call(mapper, delegateArgs);
    return maybeAugmentError(raw, normalized, mapper);
  }

  private String principalRolePath(ObjectNode args) {
    return "principal-roles/" + requireText(args, "principalRole");
  }

  private String principalRoleCatalogPath(ObjectNode args) {
    String catalog = requireText(args, "catalog");
    return principalRolePath(args) + "/catalog-roles/" + catalog;
  }

  private ObjectNode requireObject(ObjectNode args, String field, String description) {
    JsonNode node = args.get(field);
    if (!(node instanceof ObjectNode)) {
      throw new IllegalArgumentException(description + " payload (`" + field + "`) is required.");
    }
    return (ObjectNode) node;
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
        hint = "Create principal role requires CreatePrincipalRoleRequest body.";
        break;
      case "update":
        hint =
            "Update principal role requires UpdatePrincipalRoleRequest body with currentEntityVersion.";
        break;
      case "assign-catalog-role":
        hint = "Provide GrantCatalogRoleRequest body when assigning catalog roles.";
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
    if (LIST_PRINCIPALS_ALIASES.contains(operation)) {
      return "list-principals";
    }
    if (LIST_CATALOG_ROLES_ALIASES.contains(operation)) {
      return "list-catalog-roles";
    }
    if (ASSIGN_CATALOG_ROLE_ALIASES.contains(operation)) {
      return "assign-catalog-role";
    }
    if (REVOKE_CATALOG_ROLE_ALIASES.contains(operation)) {
      return "revoke-catalog-role";
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
