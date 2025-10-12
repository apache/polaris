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

"""Iceberg table MCP tool."""

from __future__ import annotations

import copy
from typing import Any, Dict, Optional, Set
from urllib.parse import quote

import urllib3

from ..authorization import AuthorizationProvider
from ..base import JSONDict, McpTool, ToolExecutionResult
from ..rest import PolarisRestTool


class PolarisTableTool(McpTool):
    """Expose Polaris table REST endpoints through MCP."""

    TOOL_NAME = "polaris-iceberg-table"
    TOOL_DESCRIPTION = (
        "Perform table-centric operations (list, get, create, commit, delete) using the Polaris REST API."
    )

    LIST_ALIASES: Set[str] = {"list", "ls"}
    GET_ALIASES: Set[str] = {"get", "load", "fetch"}
    CREATE_ALIASES: Set[str] = {"create"}
    COMMIT_ALIASES: Set[str] = {"commit", "update"}
    DELETE_ALIASES: Set[str] = {"delete", "drop"}

    def __init__(
        self,
        base_url: str,
        http: urllib3.PoolManager,
        authorization_provider: AuthorizationProvider,
    ) -> None:
        self._delegate = PolarisRestTool(
            name="polaris.table.delegate",
            description="Internal delegate for table operations",
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
                    "enum": ["list", "get", "create", "commit", "delete"],
                    "description": (
                        "Table operation to execute. Supported values: list, get (synonyms: load, fetch), "
                        "create, commit (synonym: update), delete (synonym: drop)."
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
                        "Namespace that contains the target tables. Provide as a dot-delimited string "
                        '(e.g. "analytics.daily") or an array of strings.'
                    ),
                },
                "table": {
                    "type": "string",
                    "description": (
                        "Table identifier for operations that target a specific table (get, commit, delete)."
                    ),
                },
                "query": {
                    "type": "object",
                    "description": "Optional query string parameters (for example page-size, page-token, include-drop).",
                    "additionalProperties": {"type": "string"},
                },
                "headers": {
                    "type": "object",
                    "description": "Optional additional HTTP headers to include with the request.",
                    "additionalProperties": {"type": "string"},
                },
                "body": {
                    "type": "object",
                    "description": "Optional request body payload for create or commit operations.",
                },
            },
            "required": ["operation", "catalog", "namespace"],
        }

    def call(self, arguments: Any) -> ToolExecutionResult:
        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be a JSON object.")

        operation = self._require_text(arguments, "operation").lower().strip()
        normalized = self._normalize_operation(operation)

        catalog = self._encode_segment(self._require_text(arguments, "catalog"))
        namespace = self._encode_segment(self._resolve_namespace(arguments.get("namespace")))

        delegate_args: JSONDict = {}
        self._copy_if_object(arguments.get("query"), delegate_args, "query")
        self._copy_if_object(arguments.get("headers"), delegate_args, "headers")

        if normalized == "list":
            self._handle_list(delegate_args, catalog, namespace)
        elif normalized == "get":
            self._handle_get(arguments, delegate_args, catalog, namespace)
        elif normalized == "create":
            self._handle_create(arguments, delegate_args, catalog, namespace)
        elif normalized == "commit":
            self._handle_commit(arguments, delegate_args, catalog, namespace)
        elif normalized == "delete":
            self._handle_delete(arguments, delegate_args, catalog, namespace)
        else:  # pragma: no cover - defensive, normalize guarantees handled cases
            raise ValueError(f"Unsupported operation: {operation}")

        return self._delegate.call(delegate_args)

    def _handle_list(self, delegate_args: JSONDict, catalog: str, namespace: str) -> None:
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/tables"

    def _handle_get(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        table = self._encode_segment(
            self._require_text(arguments, "table", "Table name is required for get operations.")
        )
        delegate_args["method"] = "GET"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/tables/{table}"

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
                "Create operations require a request body that matches the CreateTableRequest schema."
            )
        delegate_args["method"] = "POST"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/tables"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_commit(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        body = arguments.get("body")
        if not isinstance(body, dict):
            raise ValueError(
                "Commit operations require a request body that matches the CommitTableRequest schema."
            )
        table = self._encode_segment(
            self._require_text(arguments, "table", "Table name is required for commit operations.")
        )
        delegate_args["method"] = "POST"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/tables/{table}"
        delegate_args["body"] = copy.deepcopy(body)

    def _handle_delete(
        self,
        arguments: Dict[str, Any],
        delegate_args: JSONDict,
        catalog: str,
        namespace: str,
    ) -> None:
        table = self._encode_segment(
            self._require_text(arguments, "table", "Table name is required for delete operations.")
        )
        delegate_args["method"] = "DELETE"
        delegate_args["path"] = f"{catalog}/namespaces/{namespace}/tables/{table}"

    def _normalize_operation(self, operation: str) -> str:
        if operation in self.LIST_ALIASES:
            return "list"
        if operation in self.GET_ALIASES:
            return "get"
        if operation in self.CREATE_ALIASES:
            return "create"
        if operation in self.COMMIT_ALIASES:
            return "commit"
        if operation in self.DELETE_ALIASES:
            return "delete"
        raise ValueError(f"Unsupported operation: {operation}")

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

    def _copy_if_object(self, source: Any, target: JSONDict, field: str) -> None:
        if isinstance(source, dict):
            target[field] = copy.deepcopy(source)

    def _encode_segment(self, value: str) -> str:
        return quote(value, safe="").replace("+", "%20")

    def _require_text(self, node: Dict[str, Any], field: str, message: Optional[str] = None) -> str:
        value = node.get(field)
        if not isinstance(value, str) or not value.strip():
            if message is None:
                message = f"Missing required field: {field}"
            raise ValueError(message)
        return value.strip()
