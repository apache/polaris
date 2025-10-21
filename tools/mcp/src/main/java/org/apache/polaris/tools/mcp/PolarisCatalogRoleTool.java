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

/** MCP tool for catalog role and grant management. */
final class PolarisCatalogRoleTool implements McpTool {
  private static final String TOOL_NAME = "polaris-catalog-role-request";
  private static final String TOOL_DESCRIPTION =
      "Manage catalog roles and grants via the Polaris management API.";

  private static final Set<String> LIST_ALIASES = Set.of("list");
  private static final Set<String> CREATE_ALIASES = Set.of("create");
  private static final Set<String> GET_ALIASES = Set.of("get");
  private static final Set<String> UPDATE_ALIASES = Set.of("update");
  private static final Set<String> DELETE_ALIASES = Set.of("delete", "remove");
  private static final Set<String> LIST_PRINCIPAL_ROLES_ALIASES =
      Set.of("list-principal-roles", "list-assigned-principal-roles");
  private static final Set<String> LIST_GRANTS_ALIASES = Set.of("list-grants");
  private static final Set<String> ADD_GRANT_ALIASES = Set.of("add-grant", "grant");
  private static final Set<String> REVOKE_GRANT_ALIASES = Set.of("revoke-grant");

  private final PolarisRestTool delegate;

  PolarisCatalogRoleTool(
      ObjectMapper mapper,
      HttpExecutor executor,
      URI baseUri,
      AuthorizationProvider authorizationProvider) {
    this.delegate =
        new PolarisRestTool(
            "polaris.catalogrole.delegate",
            "Internal delegate for catalog role operations",
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
    opEnum.add("list-principal-roles");
    opEnum.add("list-grants");
    opEnum.add("add-grant");
    opEnum.add("revoke-grant");
    operation.put(
        "description",
        "Catalog role operation (list, get, create, update, delete, list-principal-roles, list-grants, add-grant, revoke-grant).");
    properties.set("operation", operation);

    ObjectNode catalog = mapper.createObjectNode();
    catalog.put("type", "string");
    catalog.put("description", "Catalog name (required).");
    properties.set("catalog", catalog);

    ObjectNode catalogRole = mapper.createObjectNode();
    catalogRole.put("type", "string");
    catalogRole.put("description", "Catalog role name for role-specific operations.");
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
        "Optional request body for create/update and grant operations. See polaris-management-service.yml for schemas like CreateCatalogRoleRequest, AddGrantRequest.");
    body.put("type", mapper.createArrayNode().add("object").add("null"));
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

    String catalog = requireText(args, "catalog");
    ObjectNode delegateArgs = mapper.createObjectNode();
    copyIfObject(args.get("query"), delegateArgs, "query");
    copyIfObject(args.get("headers"), delegateArgs, "headers");

    switch (normalized) {
      case "list":
        delegateArgs.put("method", "GET");
        delegateArgs.put("path", catalogRolesBase(catalog));
        break;
      case "create":
        delegateArgs.put("method", "POST");
        delegateArgs.put("path", catalogRolesBase(catalog));
        delegateArgs.set("body", requireObject(args, "body", "CreateCatalogRoleRequest"));
        break;
      case "get":
        delegateArgs.put("method", "GET");
        delegateArgs.put("path", catalogRolePath(catalog, args));
        break;
      case "update":
        delegateArgs.put("method", "PUT");
        delegateArgs.put("path", catalogRolePath(catalog, args));
        delegateArgs.set("body", requireObject(args, "body", "UpdateCatalogRoleRequest"));
        break;
      case "delete":
        delegateArgs.put("method", "DELETE");
        delegateArgs.put("path", catalogRolePath(catalog, args));
        break;
      case "list-principal-roles":
        delegateArgs.put("method", "GET");
        delegateArgs.put("path", catalogRolePath(catalog, args) + "/principal-roles");
        break;
      case "list-grants":
        delegateArgs.put("method", "GET");
        delegateArgs.put("path", catalogRolePath(catalog, args) + "/grants");
        break;
      case "add-grant":
        delegateArgs.put("method", "PUT");
        delegateArgs.put("path", catalogRolePath(catalog, args) + "/grants");
        delegateArgs.set("body", requireObject(args, "body", "AddGrantRequest"));
        break;
      case "revoke-grant":
        delegateArgs.put("method", "POST");
        delegateArgs.put("path", catalogRolePath(catalog, args) + "/grants");
        if (args.get("body") instanceof ObjectNode) {
          delegateArgs.set("body", args.get("body").deepCopy());
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + operation);
    }

    ToolExecutionResult raw = delegate.call(mapper, delegateArgs);
    return maybeAugmentError(raw, normalized, mapper);
  }

  private String catalogRolesBase(String catalog) {
    return "catalogs/" + catalog + "/catalog-roles";
  }

  private String catalogRolePath(String catalog, ObjectNode args) {
    return catalogRolesBase(catalog) + "/" + requireText(args, "catalogRole");
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
        hint = "Create catalog role requires CreateCatalogRoleRequest body.";
        break;
      case "update":
        hint =
            "Update catalog role requires UpdateCatalogRoleRequest body with currentEntityVersion.";
        break;
      case "add-grant":
        hint = "Grant operations require AddGrantRequest body.";
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
    if (LIST_PRINCIPAL_ROLES_ALIASES.contains(operation)) {
      return "list-principal-roles";
    }
    if (LIST_GRANTS_ALIASES.contains(operation)) {
      return "list-grants";
    }
    if (ADD_GRANT_ALIASES.contains(operation)) {
      return "add-grant";
    }
    if (REVOKE_GRANT_ALIASES.contains(operation)) {
      return "revoke-grant";
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
