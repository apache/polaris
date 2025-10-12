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

"""Namespace MCP tool."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote

import urllib3

from ..authorization import AuthorizationProvider
from ..base import JSONDict, McpTool, ToolExecutionResult
from ..rest import PolarisRestTool


class PolarisNamespaceTool(McpTool):
    """Manage namespaces through the Polaris REST API."""

    TOOL_NAME = "polaris-namespace-request"
    TOOL_DESCRIPTION = (
        "Manage namespaces in an Iceberg catalog (list, get, create, update properties, delete)."
    )

    LIST_ALIASES: Set[str] = {"list"}
    GET_ALIASES: Set[str] = {"get", "load"}
    EXISTS_ALIASES: Set[str] = {"exists", "head"}
    CREATE_ALIASES: Set[str] = {"create"}
    UPDATE_PROPS_ALIASES: Set[str] = {"update-properties", "set-properties", "properties-update"}
    GET_PROPS_ALIASES: Set[str] = {"get-properties", "properties"}
    DELETE_ALIASES: Set[str] = {"delete", "drop", "remove"}

    def __init__(
        self,
        base_url: str,
        http: urllib3.PoolManager,
        authorization_provider: AuthorizationProvider,
    ) -> None:
        self._delegate = PolarisRestTool(
            name="polaris.namespace.delegate",
            description="Internal delegate for namespace operations",
            base_url=base_url,
            default_path_prefix="api/catalog/v1/",
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
                        "get",
                        "exists",
                        "create",
                        "update-properties",
                        "get-properties",
                        "delete",
                    ],
                    "description": (
                        "Namespace operation to execute. Supported values: list, get, exists, create, "
                        "update-properties, get-properties, delete."
                    ),
                },
                "catalog": {
                    "type": "string",
                    "description": "Catalog identifier (maps to the {prefix} path segment).",
                },
                "namespace": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "array", "items": {"type": "string"}},
                    ],
                    "description": (
                        "Namespace identifier. Provide as dot-delimited string (e.g. \"analytics.daily\") "
                        "or array of path components."
                    ),
                },
                "query": {
                    "type": "object",
                    "description": "Optional query string parameters (for example page-size, page-token).",
                    "additionalProperties": {"type": "string"},
                },
                "headers": {
                    "type": "object",
                    "description": "Optional request headers.",
                    "additionalProperties": {"type": "string"},
                },
                "body": {
                    "type": "object",
                    "description": (
                        "Optional request body payload (required for create and update-properties). "
                        "See the Iceberg REST catalog specification for the expected schema."
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
        delegate_args: JSONDict = {}
        self._copy_if_object(arguments.get("query"), delegate_args, "query")
        self._copy_if_object(arguments.get("headers"), delegate_args, "headers")

        if normalized == "list":
            self._handle_list(delegate_args, catalog)
        elif normalized == "get":
            self._handle_get(arguments, delegate_args, catalog)
        elif normalized == "exists":
            self._handle_exists(arguments, delegate_args, catalog)
        elif normalized == "create":
            self._handle_create(arguments, delegate_args, catalog)
        elif normalized == "update-properties":
            self._handle_update_properties(arguments, delegate_args, catalog)
        elif normalized == "get-properties":
            self._handle_get_properties(arguments, delegate_args, catalog)
        elif normalized == "delete":
            self._handle_delete(arguments, delegate_args, catalog)
        else:  # pragma: no cover - normalize guarantees cases
            raise ValueError(f"Unsupported operation: {operation}")

        raw = self._delegate.call(delegate_args)
        return self._maybe_augment_error(raw, normalized)

    def _handle_list(self, delegate_args: JSONDict, catalog: str) -> None:
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"{catalog}/namespaces"

    def _handle_get(self, arguments: Dict[str, Any], delegate_args: JSONDict, catalog: str) -> None:
        namespace = self._resolve_namespace_path(arguments)
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}"

    def _handle_exists(
        self, arguments: Dict[str, Any], delegate_args: JSONDict, catalog: str
    ) -> None:
        namespace = self._resolve_namespace_path(arguments)
        delegate_args["method"] = "HEAD"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}"

    def _handle_create(
        self, arguments: Dict[str, Any], delegate_args: JSONDict, catalog: str
    ) -> None:
        body = arguments.get("body")
        body_obj = copy.deepcopy(body) if isinstance(body, dict) else {}

        if "namespace" not in body_obj or body_obj.get("namespace") is None:
            if "namespace" in arguments and arguments["namespace"] is not None:
                namespace_parts = self._resolve_namespace_array(arguments)
                body_obj["namespace"] = namespace_parts
            else:
                raise ValueError(
                    "Create operations require `body.namespace` or the `namespace` argument."
                )

        delegate_args["method"] = "POST"
        delegate_args["path"] = f"{catalog}/namespaces"
        delegate_args["body"] = body_obj

    def _handle_update_properties(
        self, arguments: Dict[str, Any], delegate_args: JSONDict, catalog: str
    ) -> None:
        namespace = self._resolve_namespace_path(arguments)
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError(
                "update-properties requires a body matching UpdateNamespacePropertiesRequest."
            )
        delegate_args["method"] = "POST"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/properties"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_get_properties(
        self, arguments: Dict[str, Any], delegate_args: JSONDict, catalog: str
    ) -> None:
        namespace = self._resolve_namespace_path(arguments)
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/properties"

    def _handle_delete(
        self, arguments: Dict[str, Any], delegate_args: JSONDict, catalog: str
    ) -> None:
        namespace = self._resolve_namespace_path(arguments)
        delegate_args["method"] = "DELETE"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}"

    def _maybe_augment_error(self, result: ToolExecutionResult, operation: str) -> ToolExecutionResult:
        if not result.is_error:
            return result

        metadata = copy.deepcopy(result.metadata) if result.metadata is not None else {}
        status = int(metadata.get("response", {}).get("status", -1))
        if status not in (400, 422):
            return result

        hint: Optional[str] = None
        if operation == "create":
            hint = (
                "Create requests must include `namespace` (array of strings) in the body and optional `properties`. "
                "See CreateNamespaceRequest in spec/iceberg-rest-catalog-open-api.yaml."
            )
        elif operation == "update-properties":
            hint = (
                "update-properties requests require `body` with `updates` and/or `removals`. "
                "See UpdateNamespacePropertiesRequest in spec/iceberg-rest-catalog-open-api.yaml."
            )

        if not hint:
            return result

        metadata["hint"] = hint
        text = result.text
        if hint not in text:
            text = f"{text}\nHint: {hint}"
        return ToolExecutionResult(text=text, is_error=True, metadata=metadata)

    def _resolve_namespace_array(self, arguments: Dict[str, Any]) -> List[str]:
        namespace = arguments.get("namespace")
        if namespace is None:
            raise ValueError("Namespace must be provided.")
        if isinstance(namespace, list):
            if not namespace:
                raise ValueError("Namespace array must contain at least one component.")
            parts: List[str] = []
            for element in namespace:
                if not isinstance(element, str) or not element.strip():
                    raise ValueError("Namespace array elements must be non-empty strings.")
                parts.append(element.strip())
            return parts
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("Namespace must be a non-empty string.")
        return namespace.strip().split(".")

    def _resolve_namespace_path(self, arguments: Dict[str, Any]) -> str:
        parts = self._resolve_namespace_array(arguments)
        joined = ".".join(parts)
        return self._encode_segment(joined)

    def _copy_if_object(self, source: Any, target: JSONDict, field: str) -> None:
        if isinstance(source, dict):
            target[field] = copy.deepcopy(source)

    def _normalize_operation(self, operation: str) -> str:
        if operation in self.LIST_ALIASES:
            return "list"
        if operation in self.GET_ALIASES:
            return "get"
        if operation in self.EXISTS_ALIASES:
            return "exists"
        if operation in self.CREATE_ALIASES:
            return "create"
        if operation in self.UPDATE_PROPS_ALIASES:
            return "update-properties"
        if operation in self.GET_PROPS_ALIASES:
            return "get-properties"
        if operation in self.DELETE_ALIASES:
            return "delete"
        raise ValueError(f"Unsupported operation: {operation}")

    def _require_text(self, node: Dict[str, Any], field: str) -> str:
        value = node.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Missing required field: {field}")
        return value.strip()

    def _encode_segment(self, value: str) -> str:
        return quote(value, safe="").replace("+", "%20")
