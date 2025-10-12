#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Principal MCP tool."""

from __future__ import annotations

import copy
from typing import Any, Dict, Optional, Set

import urllib3

from ..authorization import AuthorizationProvider
from ..base import JSONDict, McpTool, ToolExecutionResult
from ..rest import PolarisRestTool


class PolarisPrincipalTool(McpTool):
    """Manage principals via the Polaris management API."""

    TOOL_NAME = "polaris-principal-request"
    TOOL_DESCRIPTION = (
        "Manage principals via the Polaris management API (list, get, create, update, delete, "
        "rotate/reset credentials, role assignment)."
    )

    LIST_ALIASES: Set[str] = {"list"}
    CREATE_ALIASES: Set[str] = {"create"}
    GET_ALIASES: Set[str] = {"get"}
    UPDATE_ALIASES: Set[str] = {"update"}
    DELETE_ALIASES: Set[str] = {"delete", "remove"}
    ROTATE_ALIASES: Set[str] = {"rotate-credentials", "rotate"}
    RESET_ALIASES: Set[str] = {"reset-credentials", "reset"}
    LIST_ROLES_ALIASES: Set[str] = {"list-principal-roles", "list-roles"}
    ASSIGN_ROLE_ALIASES: Set[str] = {"assign-principal-role", "assign-role"}
    REVOKE_ROLE_ALIASES: Set[str] = {"revoke-principal-role", "revoke-role"}

    def __init__(
        self,
        base_url: str,
        http: urllib3.PoolManager,
        authorization_provider: AuthorizationProvider,
    ) -> None:
        self._delegate = PolarisRestTool(
            name="polaris.principal.delegate",
            description="Internal delegate for principal operations",
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
                        "rotate-credentials",
                        "reset-credentials",
                        "list-principal-roles",
                        "assign-principal-role",
                        "revoke-principal-role",
                    ],
                    "description": (
                        "Principal operation to execute (list, get, create, update, delete, rotate-credentials, "
                        "reset-credentials, list-principal-roles, assign-principal-role, revoke-principal-role). "
                        "Optional query parameters (e.g. catalog context) can be supplied via `query`."
                    ),
                },
                "principal": {
                    "type": "string",
                    "description": "Principal name for operations targeting a specific principal.",
                },
                "principalRole": {
                    "type": "string",
                    "description": "Principal role name for assignment/revocation operations.",
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
                        "Optional request body payload. Required for create/update, grant/revoke operations. "
                        "See polaris-management-service.yml for schemas such as CreatePrincipalRequest."
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
            self._handle_list(delegate_args)
        elif normalized == "create":
            self._handle_create(arguments, delegate_args)
        elif normalized == "get":
            self._handle_get(arguments, delegate_args)
        elif normalized == "update":
            self._handle_update(arguments, delegate_args)
        elif normalized == "delete":
            self._handle_delete(arguments, delegate_args)
        elif normalized == "rotate-credentials":
            self._handle_rotate(arguments, delegate_args)
        elif normalized == "reset-credentials":
            self._handle_reset(arguments, delegate_args)
        elif normalized == "list-principal-roles":
            self._handle_list_roles(arguments, delegate_args)
        elif normalized == "assign-principal-role":
            self._handle_assign_role(arguments, delegate_args)
        elif normalized == "revoke-principal-role":
            self._handle_revoke_role(arguments, delegate_args)
        else:  # pragma: no cover
            raise ValueError(f"Unsupported operation: {operation}")

        raw = self._delegate.call(delegate_args)
        return self._maybe_augment_error(raw, normalized)

    def _handle_list(self, delegate_args: JSONDict) -> None:
        delegate_args["method"] = "GET"
        delegate_args["path"] = "principals"

    def _handle_create(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError("Create principal requires a body matching CreatePrincipalRequest.")
        delegate_args["method"] = "POST"
        delegate_args["path"] = "principals"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_get(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        principal = self._require_text(arguments, "principal")
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"principals/{principal}"

    def _handle_update(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        principal = self._require_text(arguments, "principal")
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError("Update principal requires a body matching UpdatePrincipalRequest.")
        delegate_args["method"] = "PUT"
        delegate_args["path"] = f"principals/{principal}"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_delete(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        principal = self._require_text(arguments, "principal")
        delegate_args["method"] = "DELETE"
        delegate_args["path"] = f"principals/{principal}"

    def _handle_rotate(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        principal = self._require_text(arguments, "principal")
        delegate_args["method"] = "POST"
        delegate_args["path"] = f"principals/{principal}/rotate"

    def _handle_reset(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        principal = self._require_text(arguments, "principal")
        delegate_args["method"] = "POST"
        delegate_args["path"] = f"principals/{principal}/reset"
        if isinstance(arguments.get("body"), dict):
            delegate_args["body"] = copy.deepcopy(arguments["body"])

    def _handle_list_roles(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        principal = self._require_text(arguments, "principal")
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"principals/{principal}/principal-roles"

    def _handle_assign_role(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        principal = self._require_text(arguments, "principal")
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError(
                "assign-principal-role requires a body matching GrantPrincipalRoleRequest."
            )
        delegate_args["method"] = "PUT"
        delegate_args["path"] = f"principals/{principal}/principal-roles"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_revoke_role(self, arguments: Dict[str, Any], delegate_args: JSONDict) -> None:
        principal = self._require_text(arguments, "principal")
        role = self._require_text(arguments, "principalRole")
        delegate_args["method"] = "DELETE"
        delegate_args["path"] = f"principals/{principal}/principal-roles/{role}"

    def _maybe_augment_error(self, result: ToolExecutionResult, operation: str) -> ToolExecutionResult:
        if not result.is_error:
            return result
        metadata = copy.deepcopy(result.metadata) if result.metadata is not None else {}
        status = int(metadata.get("response", {}).get("status", -1))
        if status not in (400, 409):
            return result

        hint: Optional[str] = None
        if operation == "create":
            hint = (
                "Create principal requires a body matching CreatePrincipalRequest. "
                "See spec/polaris-management-service.yml."
            )
        elif operation == "update":
            hint = (
                "Update principal requires `principal` and body matching UpdatePrincipalRequest with currentEntityVersion."
            )
        elif operation == "assign-principal-role":
            hint = (
                "Provide GrantPrincipalRoleRequest in the body (principalRoleName, catalogName, etc.)."
            )

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
        if operation in self.ROTATE_ALIASES:
            return "rotate-credentials"
        if operation in self.RESET_ALIASES:
            return "reset-credentials"
        if operation in self.LIST_ROLES_ALIASES:
            return "list-principal-roles"
        if operation in self.ASSIGN_ROLE_ALIASES:
            return "assign-principal-role"
        if operation in self.REVOKE_ROLE_ALIASES:
            return "revoke-principal-role"
        raise ValueError(f"Unsupported operation: {operation}")

    def _copy_if_object(self, source: Any, target: JSONDict, field: str) -> None:
        if isinstance(source, dict):
            target[field] = copy.deepcopy(source)

    def _require_text(self, node: Dict[str, Any], field: str) -> str:
        value = node.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Missing required field: {field}")
        return value.strip()
