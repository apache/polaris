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
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Policy MCP tool."""

from __future__ import annotations

import copy
from typing import Any, Dict, Optional, Set
from urllib.parse import quote

import urllib3

from ..authorization import AuthorizationProvider
from ..base import JSONDict, McpTool, ToolExecutionResult
from ..rest import PolarisRestTool


class PolarisPolicyTool(McpTool):
    """Expose Polaris policy endpoints via MCP."""

    TOOL_NAME = "polaris-policy"
    TOOL_DESCRIPTION = (
        "Manage Polaris policies (list, create, update, delete, attach, detach, applicable)."
    )

    LIST_ALIASES: Set[str] = {"list"}
    GET_ALIASES: Set[str] = {"get", "load", "fetch"}
    CREATE_ALIASES: Set[str] = {"create"}
    UPDATE_ALIASES: Set[str] = {"update"}
    DELETE_ALIASES: Set[str] = {"delete", "drop", "remove"}
    ATTACH_ALIASES: Set[str] = {"attach", "map"}
    DETACH_ALIASES: Set[str] = {"detach", "unmap", "unattach"}
    APPLICABLE_ALIASES: Set[str] = {"applicable", "applicable-policies"}

    def __init__(
        self,
        base_url: str,
        http: urllib3.PoolManager,
        authorization_provider: AuthorizationProvider,
    ) -> None:
        self._delegate = PolarisRestTool(
            name="polaris.policy.delegate",
            description="Internal delegate for policy operations",
            base_url=base_url,
            default_path_prefix="api/catalog/polaris/v1/",
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
                    "enum": ["list", "get", "create", "update", "delete", "attach", "detach", "applicable"],
                    "description": (
                        "Policy operation to execute. Supported values: list, get, create, update, delete, attach, detach, applicable."
                    ),
                },
                "catalog": {
                    "type": "string",
                    "description": "Polaris catalog identifier (maps to the {prefix} path segment).",
                },
                "namespace": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "array", "items": {"type": "string"}},
                    ],
                    "description": (
                        "Namespace that contains the target policies. Provide as a dot-delimited string "
                        '(e.g. "analytics.daily") or an array of strings.'
                    ),
                },
                "policy": {
                    "type": "string",
                    "description": "Policy identifier for operations that target a specific policy.",
                },
                "query": {
                    "type": "object",
                    "description": (
                        "Optional query string parameters (for example page-size, policy-type, detach-all)."
                    ),
                    "additionalProperties": {"type": "string"},
                },
                "headers": {
                    "type": "object",
                    "description": "Optional additional HTTP headers to include with the request.",
                    "additionalProperties": {"type": "string"},
                },
                "body": {
                    "type": "object",
                    "description": (
                        "Optional request body payload for create/update/attach/detach operations. "
                        "The structure must follow the corresponding Polaris REST schema."
                    ),
                },
            },
            "required": ["operation", "catalog"],
        }

    def call(self, arguments: Any) -> ToolExecutionResult:
        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be a JSON object.")

        operation = self._require_text(arguments, "operation").lower().strip()
        normalized = self._normalize_operation(operation)

        catalog = self._encode_segment(self._require_text(arguments, "catalog"))
        namespace: Optional[str] = None
        if normalized != "applicable":
            namespace = self._encode_segment(self._resolve_namespace(arguments.get("namespace")))
        elif arguments.get("namespace") is not None:
            namespace = self._encode_segment(self._resolve_namespace(arguments.get("namespace")))

        delegate_args: JSONDict = {}
        self._copy_if_object(arguments.get("query"), delegate_args, "query")
        self._copy_if_object(arguments.get("headers"), delegate_args, "headers")

        if normalized == "list":
            self._require_namespace(namespace, "list")
            self._handle_list(delegate_args, catalog, namespace)
        elif normalized == "get":
            self._require_namespace(namespace, "get")
            self._handle_get(arguments, delegate_args, catalog, namespace)
        elif normalized == "create":
            self._require_namespace(namespace, "create")
            self._handle_create(arguments, delegate_args, catalog, namespace)
        elif normalized == "update":
            self._require_namespace(namespace, "update")
            self._handle_update(arguments, delegate_args, catalog, namespace)
        elif normalized == "delete":
            self._require_namespace(namespace, "delete")
            self._handle_delete(arguments, delegate_args, catalog, namespace)
        elif normalized == "attach":
            self._require_namespace(namespace, "attach")
            self._handle_attach(arguments, delegate_args, catalog, namespace)
        elif normalized == "detach":
            self._require_namespace(namespace, "detach")
            self._handle_detach(arguments, delegate_args, catalog, namespace)
        elif normalized == "applicable":
            self._handle_applicable(delegate_args, catalog)
        else:  # pragma: no cover
            raise ValueError(f"Unsupported operation: {operation}")

        raw = self._delegate.call(delegate_args)
        return self._maybe_augment_error(raw, normalized)

    def _handle_list(self, delegate_args: JSONDict, catalog: str, namespace: str) -> None:
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/policies"

    def _handle_get(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        policy = self._encode_segment(
            self._require_text(arguments, "policy", "Policy name is required for get operations.")
        )
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/policies/{policy}"

    def _handle_create(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError(
                "Create operations require a request body that matches the CreatePolicyRequest schema."
            )
        delegate_args["method"] = "POST"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/policies"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_update(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError(
                "Update operations require a request body that matches the UpdatePolicyRequest schema."
            )
        policy = self._encode_segment(
            self._require_text(arguments, "policy", "Policy name is required for update operations.")
        )
        delegate_args["method"] = "PUT"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/policies/{policy}"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_delete(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        policy = self._encode_segment(
            self._require_text(arguments, "policy", "Policy name is required for delete operations.")
        )
        delegate_args["method"] = "DELETE"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/policies/{policy}"

    def _handle_attach(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError(
                "Attach operations require a request body that matches the AttachPolicyRequest schema."
            )
        policy = self._encode_segment(
            self._require_text(arguments, "policy", "Policy name is required for attach operations.")
        )
        delegate_args["method"] = "PUT"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/policies/{policy}/mappings"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_detach(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError(
                "Detach operations require a request body that matches the DetachPolicyRequest schema."
            )
        policy = self._encode_segment(
            self._require_text(arguments, "policy", "Policy name is required for detach operations.")
        )
        delegate_args["method"] = "POST"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/policies/{policy}/mappings"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_applicable(self, delegate_args: JSONDict, catalog: str) -> None:
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"{catalog}/applicable-policies"

    def _maybe_augment_error(self, result: ToolExecutionResult, operation: str) -> ToolExecutionResult:
        if not result.is_error:
            return result
        metadata = copy.deepcopy(result.metadata) if result.metadata is not None else {}
        status = int(metadata.get("response", {}).get("status", -1))
        if status not in (400, 404, 422):
            return result

        hint: Optional[str] = None
        if operation == "create":
            hint = (
                "Create requests must include `name`, `type`, and optional `description`/`content` in the body. "
                "See CreatePolicyRequest in spec/polaris-catalog-apis/policy-apis.yaml. "
                "Common types include system.data-compaction, system.metadata-compaction, "
                "system.orphan-file-removal, and system.snapshot-expiry. "
                "Example: {\"name\":\"weekly_compaction\",\"type\":\"system.data-compaction\",\"content\":{...}}. "
                "Reference schema: http://polaris.apache.org/schemas/policies/system/data-compaction/2025-02-03.json"
            )
        elif operation == "update":
            hint = (
                "Update requests require the policy name in the path and the body with `description`, "
                "`content`, and `currentVersion`."
            )
        elif operation == "attach":
            hint = (
                "Attach requests require a body with `targetType`, `targetName`, and optional `parameters`. "
                "Ensure the policy exists first (create it with operation=create) before attaching."
            )
        elif operation == "detach":
            hint = (
                "Detach requests require a body with `targetType`, `targetName`, and optional `parameters`."
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
        if operation in self.GET_ALIASES:
            return "get"
        if operation in self.CREATE_ALIASES:
            return "create"
        if operation in self.UPDATE_ALIASES:
            return "update"
        if operation in self.DELETE_ALIASES:
            return "delete"
        if operation in self.ATTACH_ALIASES:
            return "attach"
        if operation in self.DETACH_ALIASES:
            return "detach"
        if operation in self.APPLICABLE_ALIASES:
            return "applicable"
        raise ValueError(f"Unsupported operation: {operation}")

    def _copy_if_object(self, source: Any, target: JSONDict, field: str) -> None:
        if isinstance(source, dict):
            target[field] = copy.deepcopy(source)

    def _require_text(self, node: Dict[str, Any], field: str, message: Optional[str] = None) -> str:
        value = node.get(field)
        if not isinstance(value, str) or not value.strip():
            if message is None:
                message = f"Missing required field: {field}"
            raise ValueError(message)
        return value.strip()

    def _require_namespace(self, namespace: Optional[str], operation: str) -> None:
        if not namespace:
            raise ValueError(
                f"Namespace is required for {operation} operations. Provide `namespace` as a string or array."
            )

    def _resolve_namespace(self, namespace: Any) -> str:
        if namespace is None:
            raise ValueError("Namespace must be provided.")
        if isinstance(namespace, list):
            if not namespace:
                raise ValueError("Namespace array must contain at least one element.")
            parts = []
            for element in namespace:
                if not isinstance(element, str) or not element.strip():
                    raise ValueError("Namespace array elements must be non-empty strings.")
                parts.append(element.strip())
            return ".".join(parts)
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("Namespace must be a non-empty string.")
        return namespace.strip()

    def _encode_segment(self, value: str) -> str:
        return quote(value, safe="").replace("+", "%20")
