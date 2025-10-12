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

"""HTTP helper used by the Polaris MCP tools."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

import urllib3

from .authorization import AuthorizationProvider, none
from .base import JSONDict, ToolExecutionResult


DEFAULT_TIMEOUT = urllib3.Timeout(connect=30.0, read=30.0)


def _ensure_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else f"{url}/"


def _normalize_prefix(prefix: Optional[str]) -> str:
    if not prefix:
        return ""
    trimmed = prefix.strip()
    if trimmed.startswith("/"):
        trimmed = trimmed[1:]
    if trimmed and not trimmed.endswith("/"):
        trimmed = f"{trimmed}/"
    return trimmed


def _merge_headers(values: Optional[Dict[str, Any]]) -> Dict[str, str]:
    headers: Dict[str, str] = {"Accept": "application/json"}
    if not values:
        return headers

    for name, raw_value in values.items():
        if not name or raw_value is None:
            continue
        if isinstance(raw_value, list):
            flattened = [str(item) for item in raw_value if item is not None]
            if not flattened:
                continue
            headers[name] = ", ".join(flattened)
        else:
            headers[name] = str(raw_value)
    return headers


def _serialize_body(node: Any) -> Optional[str]:
    if node is None:
        return None
    if isinstance(node, (str, bytes)):
        return node.decode("utf-8") if isinstance(node, bytes) else node
    return json.dumps(node)


def _pretty_body(raw: str) -> str:
    if not raw.strip():
        return ""
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    return json.dumps(parsed, indent=2)


def _headers_to_dict(headers: urllib3.response.HTTPHeaderDict) -> Dict[str, str]:
    flattened: Dict[str, str] = {}
    for key in headers:
        values = headers.getlist(key)
        flattened[key] = ", ".join(values)
    return flattened


def _append_query(url: str, params: List[Tuple[str, str]]) -> str:
    if not params:
        return url
    parsed = urlsplit(url)
    extra_parts = [urlencode({k: v}) for k, v in params if v is not None]
    existing = parsed.query
    if existing:
        query = "&".join([existing] + extra_parts) if extra_parts else existing
    else:
        query = "&".join(extra_parts)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def _build_query(parameters: Optional[Dict[str, Any]]) -> List[Tuple[str, str]]:
    if not parameters:
        return []
    entries: List[Tuple[str, str]] = []
    for key, value in parameters.items():
        if not key or value is None:
            continue
        if isinstance(value, list):
            for item in value:
                if item is not None:
                    entries.append((key, str(item)))
        else:
            entries.append((key, str(value)))
    return entries


def _maybe_parse_json(text: Optional[str]) -> Tuple[Optional[Any], Optional[str]]:
    if text is None:
        return None, None
    try:
        return json.loads(text), None
    except json.JSONDecodeError:
        return None, text


class PolarisRestTool:
    """Issues HTTP requests against the Polaris REST API and packages the response."""

    def __init__(
        self,
        name: str,
        description: str,
        base_url: str,
        default_path_prefix: str,
        http: urllib3.PoolManager,
        authorization_provider: Optional[AuthorizationProvider] = None,
    ) -> None:
        self._name = name
        self._description = description
        self._base_url = _ensure_trailing_slash(base_url)
        self._path_prefix = _normalize_prefix(default_path_prefix)
        self._http = http
        self._authorization = authorization_provider or none()

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    def input_schema(self) -> JSONDict:
        """Return the generic JSON schema shared by delegated tools."""
        return {
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "description": (
                        "HTTP method, e.g. GET, POST, PUT, DELETE, PATCH, HEAD or OPTIONS. "
                        "Defaults to GET."
                    ),
                },
                "path": {
                    "type": "string",
                    "description": (
                        "Relative path under the Polaris base URL, such as "
                        "/api/management/v1/catalogs. Absolute URLs are also accepted."
                    ),
                },
                "query": {
                    "type": "object",
                    "description": (
                        "Optional query string parameters. Values can be strings or arrays of strings."
                    ),
                    "additionalProperties": {
                        "anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]
                    },
                },
                "headers": {
                    "type": "object",
                    "description": (
                        "Optional request headers. Accept and Authorization headers are supplied "
                        "automatically when omitted."
                    ),
                    "additionalProperties": {
                        "anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]
                    },
                },
                "body": {
                    "type": ["object", "array", "string", "number", "boolean", "null"],
                    "description": (
                        "Optional request body. Objects and arrays are serialized as JSON, strings "
                        "are sent as-is."
                    ),
                },
            },
            "required": ["path"],
        }

    def call(self, arguments: Any) -> ToolExecutionResult:
        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be a JSON object.")

        method = str(arguments.get("method", "GET") or "GET").strip().upper() or "GET"
        path = self._require_path(arguments)
        query_params = arguments.get("query")
        headers_param = arguments.get("headers")
        body_node = arguments.get("body")

        query = query_params if isinstance(query_params, dict) else None
        headers = headers_param if isinstance(headers_param, dict) else None

        target_uri = self._resolve_target_uri(path, query)

        header_values = _merge_headers(headers)
        if not any(name.lower() == "authorization" for name in header_values):
            token = self._authorization.authorization_header()
            if token:
                header_values["Authorization"] = token

        body_text = _serialize_body(body_node)
        if body_text is not None and not any(name.lower() == "content-type" for name in header_values):
            header_values["Content-Type"] = "application/json"

        response = self._http.request(
            method,
            target_uri,
            body=body_text.encode("utf-8") if body_text is not None else None,
            headers=header_values,
            timeout=DEFAULT_TIMEOUT,
        )

        response_body = response.data.decode("utf-8") if response.data else ""
        rendered_body = _pretty_body(response_body)

        lines = [f"{method} {target_uri}", f"Status: {response.status}"]
        for key, value in _headers_to_dict(response.headers).items():
            lines.append(f"{key}: {value}")
        if rendered_body:
            lines.append("")
            lines.append(rendered_body)
        message = "\n".join(lines)

        metadata: JSONDict = {
            "method": method,
            "url": target_uri,
            "status": response.status,
            "request": {
                "method": method,
                "url": target_uri,
                "headers": dict(header_values),
            },
            "response": {
                "status": response.status,
                "headers": _headers_to_dict(response.headers),
            },
        }

        if body_text is not None:
            parsed, fallback = _maybe_parse_json(body_text)
            if parsed is not None:
                metadata["request"]["body"] = parsed
            elif fallback is not None:
                metadata["request"]["bodyText"] = fallback

        if response_body.strip():
            parsed, fallback = _maybe_parse_json(response_body)
            if parsed is not None:
                metadata["response"]["body"] = parsed
            elif fallback is not None:
                metadata["response"]["bodyText"] = fallback

        is_error = response.status >= 400
        return ToolExecutionResult(message, is_error, metadata)

    def _require_path(self, args: Dict[str, Any]) -> str:
        path = args.get("path")
        if not isinstance(path, str) or not path.strip():
            raise ValueError("The 'path' argument must be provided and must not be empty.")
        return path.strip()

    def _resolve_target_uri(self, path: str, query: Optional[Dict[str, Any]]) -> str:
        if path.startswith(("http://", "https://")):
            target = path
        else:
            relative = path[1:] if path.startswith("/") else path
            if self._path_prefix:
                relative = f"{self._path_prefix}{relative}"
            target = urljoin(self._base_url, relative)

        params = _build_query(query)
        return _append_query(target, params)
