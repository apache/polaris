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

"""Entry point for the Polaris Model Context Protocol server."""

from __future__ import annotations

import os
from typing import Any, Mapping, MutableMapping, Sequence
from urllib.parse import urljoin

import urllib3
from fastmcp import FastMCP
from fastmcp.tools.tool import ToolResult as FastMcpToolResult
from importlib import metadata
from mcp.types import TextContent

from .authorization import (
    AuthorizationProvider,
    ClientCredentialsAuthorizationProvider,
    StaticAuthorizationProvider,
    none,
)
from .base import ToolExecutionResult
from .tools import (
    PolarisCatalogRoleTool,
    PolarisCatalogTool,
    PolarisNamespaceTool,
    PolarisPolicyTool,
    PolarisPrincipalRoleTool,
    PolarisPrincipalTool,
    PolarisTableTool,
)

DEFAULT_BASE_URL = "http://localhost:8181/"
OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "isError": {"type": "boolean"},
        "meta": {"type": "object"},
    },
    "required": ["isError"],
    "additionalProperties": True,
}


def create_server() -> FastMCP:
    """Construct a FastMCP server with Polaris tools."""

    base_url = _resolve_base_url()
    http = urllib3.PoolManager()
    authorization_provider = _resolve_authorization_provider(base_url, http)

    table_tool = PolarisTableTool(base_url, http, authorization_provider)
    namespace_tool = PolarisNamespaceTool(base_url, http, authorization_provider)
    principal_tool = PolarisPrincipalTool(base_url, http, authorization_provider)
    principal_role_tool = PolarisPrincipalRoleTool(base_url, http, authorization_provider)
    catalog_role_tool = PolarisCatalogRoleTool(base_url, http, authorization_provider)
    policy_tool = PolarisPolicyTool(base_url, http, authorization_provider)
    catalog_tool = PolarisCatalogTool(base_url, http, authorization_provider)

    server_version = _resolve_package_version()
    mcp = FastMCP(
        name="polaris-mcp",
        version=server_version,
    )

    @mcp.tool(
        name=table_tool.name,
        description=table_tool.description,
        output_schema=OUTPUT_SCHEMA,
    )
    def polaris_iceberg_table(
        operation: str,
        catalog: str,
        namespace: str | Sequence[str],
        table: str | None = None,
        query: Mapping[str, str | Sequence[str]] | None = None,
        headers: Mapping[str, str | Sequence[str]] | None = None,
        body: Any | None = None,
    ) -> FastMcpToolResult:
        return _call_tool(
            table_tool,
            required={
                "operation": operation,
                "catalog": catalog,
                "namespace": namespace,
            },
            optional={
                "table": table,
                "query": query,
                "headers": headers,
                "body": body,
            },
            transforms={
                "namespace": _normalize_namespace,
                "query": _copy_mapping,
                "headers": _copy_mapping,
                "body": _coerce_body,
            },
        )

    @mcp.tool(
        name=namespace_tool.name,
        description=namespace_tool.description,
        output_schema=OUTPUT_SCHEMA,
    )
    def polaris_namespace_request(
        operation: str,
        catalog: str,
        namespace: str | Sequence[str] | None = None,
        query: Mapping[str, str | Sequence[str]] | None = None,
        headers: Mapping[str, str | Sequence[str]] | None = None,
        body: Any | None = None,
    ) -> FastMcpToolResult:
        return _call_tool(
            namespace_tool,
            required={
                "operation": operation,
                "catalog": catalog,
            },
            optional={
                "namespace": namespace,
                "query": query,
                "headers": headers,
                "body": body,
            },
            transforms={
                "namespace": _normalize_namespace,
                "query": _copy_mapping,
                "headers": _copy_mapping,
                "body": _coerce_body,
            },
        )

    @mcp.tool(
        name=principal_tool.name,
        description=principal_tool.description,
        output_schema=OUTPUT_SCHEMA,
    )
    def polaris_principal_request(
        operation: str,
        principal: str | None = None,
        principalRole: str | None = None,
        query: Mapping[str, str | Sequence[str]] | None = None,
        headers: Mapping[str, str | Sequence[str]] | None = None,
        body: Any | None = None,
    ) -> FastMcpToolResult:
        return _call_tool(
            principal_tool,
            required={"operation": operation},
            optional={
                "principal": principal,
                "principalRole": principalRole,
                "query": query,
                "headers": headers,
                "body": body,
            },
            transforms={
                "query": _copy_mapping,
                "headers": _copy_mapping,
                "body": _coerce_body,
            },
        )

    @mcp.tool(
        name=principal_role_tool.name,
        description=principal_role_tool.description,
        output_schema=OUTPUT_SCHEMA,
    )
    def polaris_principal_role_request(
        operation: str,
        principalRole: str | None = None,
        catalog: str | None = None,
        catalogRole: str | None = None,
        query: Mapping[str, str | Sequence[str]] | None = None,
        headers: Mapping[str, str | Sequence[str]] | None = None,
        body: Any | None = None,
    ) -> FastMcpToolResult:
        return _call_tool(
            principal_role_tool,
            required={"operation": operation},
            optional={
                "principalRole": principalRole,
                "catalog": catalog,
                "catalogRole": catalogRole,
                "query": query,
                "headers": headers,
                "body": body,
            },
            transforms={
                "query": _copy_mapping,
                "headers": _copy_mapping,
                "body": _coerce_body,
            },
        )

    @mcp.tool(
        name=catalog_role_tool.name,
        description=catalog_role_tool.description,
        output_schema=OUTPUT_SCHEMA,
    )
    def polaris_catalog_role_request(
        operation: str,
        catalog: str,
        catalogRole: str | None = None,
        query: Mapping[str, str | Sequence[str]] | None = None,
        headers: Mapping[str, str | Sequence[str]] | None = None,
        body: Any | None = None,
    ) -> FastMcpToolResult:
        return _call_tool(
            catalog_role_tool,
            required={
                "operation": operation,
                "catalog": catalog,
            },
            optional={
                "catalogRole": catalogRole,
                "query": query,
                "headers": headers,
                "body": body,
            },
            transforms={
                "query": _copy_mapping,
                "headers": _copy_mapping,
                "body": _coerce_body,
            },
        )

    @mcp.tool(
        name=policy_tool.name,
        description=policy_tool.description,
        output_schema=OUTPUT_SCHEMA,
    )
    def polaris_policy_request(
        operation: str,
        catalog: str,
        namespace: str | Sequence[str] | None = None,
        policy: str | None = None,
        query: Mapping[str, str | Sequence[str]] | None = None,
        headers: Mapping[str, str | Sequence[str]] | None = None,
        body: Any | None = None,
    ) -> FastMcpToolResult:
        return _call_tool(
            policy_tool,
            required={
                "operation": operation,
                "catalog": catalog,
            },
            optional={
                "namespace": namespace,
                "policy": policy,
                "query": query,
                "headers": headers,
                "body": body,
            },
            transforms={
                "namespace": _normalize_namespace,
                "query": _copy_mapping,
                "headers": _copy_mapping,
                "body": _coerce_body,
            },
        )

    @mcp.tool(
        name=catalog_tool.name,
        description=catalog_tool.description,
        output_schema=OUTPUT_SCHEMA,
    )
    def polaris_catalog_request(
        operation: str,
        catalog: str | None = None,
        query: Mapping[str, str | Sequence[str]] | None = None,
        headers: Mapping[str, str | Sequence[str]] | None = None,
        body: Any | None = None,
    ) -> FastMcpToolResult:
        return _call_tool(
            catalog_tool,
            required={"operation": operation},
            optional={
                "catalog": catalog,
                "query": query,
                "headers": headers,
                "body": body,
            },
            transforms={
                "query": _copy_mapping,
                "headers": _copy_mapping,
                "body": _coerce_body,
            },
        )

    return mcp


def _call_tool(
    tool: Any,
    *,
    required: Mapping[str, Any],
    optional: Mapping[str, Any | None] | None = None,
    transforms: Mapping[str, Any] | None = None,
) -> FastMcpToolResult:
    arguments: MutableMapping[str, Any] = dict(required)
    if optional:
        for key, value in optional.items():
            if value is not None:
                arguments[key] = value
    if transforms:
        for key, transform in transforms.items():
            if key in arguments and arguments[key] is not None:
                arguments[key] = transform(arguments[key])
    return _to_tool_result(tool.call(arguments))


def _to_tool_result(result: ToolExecutionResult) -> FastMcpToolResult:
    structured = {"isError": result.is_error}
    if result.metadata is not None:
        structured["meta"] = result.metadata
    return FastMcpToolResult(
        content=[TextContent(type="text", text=result.text)],
        structured_content=structured,
    )


def _copy_mapping(
    mapping: Mapping[str, Any] | None,
) -> MutableMapping[str, Any] | None:
    if mapping is None:
        return None
    copied: MutableMapping[str, Any] = {}
    for key, value in mapping.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            copied[key] = [str(item) for item in value]
        else:
            copied[key] = value
    return copied


def _coerce_body(body: Any) -> Any:
    if isinstance(body, Mapping):
        return dict(body)
    return body


def _normalize_namespace(namespace: str | Sequence[str]) -> str | list[str]:
    if isinstance(namespace, str):
        return namespace
    return [str(part) for part in namespace]


def _resolve_base_url() -> str:
    for candidate in (
        os.getenv("POLARIS_BASE_URL"),
        os.getenv("POLARIS_REST_BASE_URL"),
    ):
        if candidate and candidate.strip():
            return candidate.strip()
    return DEFAULT_BASE_URL


def _resolve_authorization_provider(
    base_url: str, http: urllib3.PoolManager
) -> AuthorizationProvider:
    token = _resolve_token()
    if token:
        return StaticAuthorizationProvider(token)

    client_id = _first_non_blank(
        os.getenv("POLARIS_CLIENT_ID"),
    )
    client_secret = _first_non_blank(
        os.getenv("POLARIS_CLIENT_SECRET"),
    )

    if client_id and client_secret:
        scope = _first_non_blank(os.getenv("POLARIS_TOKEN_SCOPE"))
        token_url = _first_non_blank(os.getenv("POLARIS_TOKEN_URL"))
        endpoint = token_url or urljoin(base_url, "api/catalog/v1/oauth/tokens")
        return ClientCredentialsAuthorizationProvider(
            token_endpoint=endpoint,
            client_id=client_id,
            client_secret=client_secret,
            scope=scope,
            http=http,
        )

    return none()


def _resolve_token() -> str | None:
    return _first_non_blank(
        os.getenv("POLARIS_API_TOKEN"),
        os.getenv("POLARIS_BEARER_TOKEN"),
        os.getenv("POLARIS_TOKEN"),
    )


def _first_non_blank(*candidates: str | None) -> str | None:
    for candidate in candidates:
        if candidate and candidate.strip():
            return candidate.strip()
    return None


def _resolve_package_version() -> str:
    try:
        return metadata.version("polaris-mcp")
    except metadata.PackageNotFoundError:
        return "dev"


def main() -> None:
    """Script entry point."""

    server = create_server()
    server.run()


if __name__ == "__main__":  # pragma: no cover
    main()
