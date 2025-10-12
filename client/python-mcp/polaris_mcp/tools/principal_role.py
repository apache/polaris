#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the
# License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the License.
#

"""Principal role MCP tool."""

from __future__ import annotations

import copy
from typing import Any, Dict, Optional, Set

import urllib3

from ..authorization import AuthorizationProvider
from ..base import JSONDict, McpTool, ToolExecutionResult
from ..rest import PolarisRestTool


class PolarisPrincipalRoleTool(McpTool):
    """Manage principal roles through the Polaris management API."""

    TOOL_NAME = "polaris-principal-role-request"
    TOOL_DESCRIPTION = (
        "Manage principal roles (list, get, create, update, delete) and their catalog-role assignments via the Polaris management API."
    )

    LIST_ALIASES: Set[str] = {"list"}
    CREATE_ALIASES: Set[str] = {"create"}
    GET_ALIASES: Set[str] = {"get"}
    UPDATE_ALIASES: Set[str] = {"update"}
    DELETE_ALIASES: Set[str] = {"delete", "remove"}
    LIST_PRINCIPALS_ALIASES: Set[str] = {"list-principals", "list-assignees"}
    LIST_CATALOG_ROLES_ALIASES: Set[str] = {"list-catalog-roles", "list-mapped-catalog-roles"}
    ASSIGN_CATALOG_ROLE_ALIASES: Set[str] = {"assign-catalog-role", "grant-catalog-role"}
    REVOKE_CATALOG_ROLE_ALIASES: Set[str] = {"revoke-catalog-role", "remove-catalog-role"}

    def __init__(
        self,
        base_url: str,
        http: urllib3.PoolManager,
        authorization_provider: AuthorizationProvider,
    ) -> None:
        self._delegate = PolarisRestTool(
            name="polaris.principalrole.delegate",
            description="Internal delegate for principal role operations",
            base_url=base_url,
            default_path_prefix="api/management/v1/",
            http=http,
            authorization_provider=authorization_provider,
        )

    @property
    def name(self) -> str:
        return self.TOOL_NAME

    @property
    def description(self) -> str:
        return self.TOOL_DESCRIPTION

    def input_schema(self) -> JSONDict:
        return {
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": [
                        "list",
                        "create",
                        "get",
                        "update",
                        "delete",
                        "list-principals",
                        "list-catalog-roles",
                        "assign-catalog-role",
                        "revoke-catalog-role",
                    ],
                    "description": (
                        "Principal role operation to execute. Provide optional `catalog`/`catalogRole` when required "
                        "(e.g. assignment)."
                    ),
                },
                "principalRole": {
                    "type": "string",
                    "description": "Principal role name (required for role-specific operations).",
                },
                "catalog": {
                    "type": "string",
                    "description": "Catalog name for mapping operations to catalog roles.",
                },
                "catalogRole": {
                    "type": "string",
                    "description": "Catalog role name for revoke operations.",
                },
                "query": {
                    "type": "object",
                    "description": "Optional query string parameters.",
                    "additionalProperties": {"type": "string"},
                },
                "headers": {
                    "type": "object",
                    "description": "Optional request headers.",
                    "additionalProperties": {"type": "string"},
                },
                "body": {
                    "type": ["object", "null"],
                    "description": (
                        "Optional request body payload (required for create/update and assign operations). "
                        "See polaris-management-service.yml for request schemas."
                    ),
                },
            },
            "required": ["operation"],
        }

    def call(self, arguments: Any) -> ToolExecutionResult:
        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be a JSON object.")

        operation = self._require_text(arguments, "operation").lower().strip()
        normalized = self._normalize_operation(operation)

        delegate_args: JSONDict = {}
        self._copy_if_object(arguments.get("query"), delegate_args, "query")
        self._copy_if_object(arguments.get("headers"), delegate_args, "headers")

        if normalized == "list":
            delegate_args["method"] = "GET"
            delegate_args["path"] = "principal-roles"
        elif normalized == "create":
            delegate_args["method"] = "POST"
            delegate_args["path"] = "principal-roles"
            delegate_args["body"] = self._require_object(
                arguments, "body", "CreatePrincipalRoleRequest"
            )
        elif normalized == "get":
            delegate_args["method"] = "GET"
            delegate_args["path"] = self._principal_role_path(arguments)
        elif normalized == "update":
            delegate_args["method"] = "PUT"
            delegate_args["path"] = self._principal_role_path(arguments)
            delegate_args["body"] = self._require_object(
                arguments, "body", "UpdatePrincipalRoleRequest"
            )
        elif normalized == "delete":
            delegate_args["method"] = "DELETE"
            delegate_args["path"] = self._principal_role_path(arguments)
        elif normalized == "list-principals":
            delegate_args["method"] = "GET"
            delegate_args["path"] = f"{self._principal_role_path(arguments)}/principals"
        elif normalized == "list-catalog-roles":
            delegate_args["method"] = "GET"
            delegate_args["path"] = self._principal_role_catalog_path(arguments)
        elif normalized == "assign-catalog-role":
            delegate_args["method"] = "PUT"
            delegate_args["path"] = self._principal_role_catalog_path(arguments)
            delegate_args["body"] = self._require_object(
                arguments, "body", "GrantCatalogRoleRequest"
            )
        elif normalized == "revoke-catalog-role":
            delegate_args["method"] = "DELETE"
            catalog_role = self._require_text(arguments, "catalogRole")
            delegate_args["path"] = (
                f"{self._principal_role_catalog_path(arguments)}/{catalog_role}"
            )
        else:  # pragma: no cover
            raise ValueError(f"Unsupported operation: {operation}")

        raw = self._delegate.call(delegate_args)
        return self._maybe_augment_error(raw, normalized)

    def _principal_role_path(self, arguments: Dict[str, Any]) -> str:
        role = self._require_text(arguments, "principalRole")
        return f"principal-roles/{role}"

    def _principal_role_catalog_path(self, arguments: Dict[str, Any]) -> str:
        catalog = self._require_text(arguments, "catalog")
        return f"{self._principal_role_path(arguments)}/catalog-roles/{catalog}"

    def _require_object(self, arguments: Dict[str, Any], field: str, description: str) -> Dict[str, Any]:
        node = arguments.get(field)
        if not isinstance(node, dict):
            raise ValueError(f"{description} payload (`{field}`) is required.")
        return copy.deepcopy(node)

    def _maybe_augment_error(self, result: ToolExecutionResult, operation: str) -> ToolExecutionResult:
        if not result.is_error:
            return result
        metadata = copy.deepcopy(result.metadata) if result.metadata is not None else {}
        status = int(metadata.get("response", {}).get("status", -1))
        if status not in (400, 409):
            return result

        hint: Optional[str] = None
        if operation == "create":
            hint = "Create principal role requires CreatePrincipalRoleRequest body."
        elif operation == "update":
            hint = (
                "Update principal role requires UpdatePrincipalRoleRequest body with currentEntityVersion."
            )
        elif operation == "assign-catalog-role":
            hint = "Provide GrantCatalogRoleRequest body when assigning catalog roles."

        if not hint:
            return result

        metadata["hint"] = hint
        text = result.text
        if hint not in text:
            text = f"{text}\nHint: {hint}"
        return ToolExecutionResult(text=text, is_error=True, metadata=metadata)

    def _normalize_operation(self, operation: str) -> str:
        if operation in self.LIST_ALIASES:
            return "list"
        if operation in self.CREATE_ALIASES:
            return "create"
        if operation in self.GET_ALIASES:
            return "get"
        if operation in self.UPDATE_ALIASES:
            return "update"
        if operation in self.DELETE_ALIASES:
            return "delete"
        if operation in self.LIST_PRINCIPALS_ALIASES:
            return "list-principals"
        if operation in self.LIST_CATALOG_ROLES_ALIASES:
            return "list-catalog-roles"
        if operation in self.ASSIGN_CATALOG_ROLE_ALIASES:
            return "assign-catalog-role"
        if operation in self.REVOKE_CATALOG_ROLE_ALIASES:
            return "revoke-catalog-role"
        raise ValueError(f"Unsupported operation: {operation}")

    def _copy_if_object(self, source: Any, target: JSONDict, field: str) -> None:
        if isinstance(source, dict):
            target[field] = copy.deepcopy(source)

    def _require_text(self, node: Dict[str, Any], field: str) -> str:
        value = node.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Missing required field: {field}")
        return value.strip()
