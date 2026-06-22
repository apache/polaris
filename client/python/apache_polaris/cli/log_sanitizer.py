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
"""
Sanitize Polaris CLI debug log output.

``polaris --debug`` logs HTTP traffic to stderr. Known credential fields and
OAuth token endpoint payloads are redacted before writing.
"""

import json
import sys
from typing import Any
from urllib.parse import parse_qsl, urlencode

REDACTED = "***REDACTED***"
OAUTH_TOKEN_BODY_REDACTED = "<redacted sensitive authentication payload>"
SANITIZE_FAILURE_MESSAGE = "<redacted: unable to sanitize payload>"

SENSITIVE_BODY_KEYS = frozenset({"client_secret", "access_token", "refresh_token"})


def sanitize_data(data: Any) -> Any:
    if isinstance(data, dict):
        sanitized: dict[Any, Any] = {}
        for key, value in data.items():
            if key in SENSITIVE_BODY_KEYS:
                if isinstance(value, (dict, list, tuple)):
                    sanitized[key] = sanitize_data(value)
                else:
                    sanitized[key] = REDACTED
            else:
                sanitized[key] = sanitize_data(value)
        return sanitized
    if isinstance(data, list):
        return [sanitize_data(item) for item in data]
    if isinstance(data, tuple):
        return tuple(sanitize_data(item) for item in data)
    return data


def sanitize_headers(headers: dict[str, Any] | None) -> dict[str, Any] | None:
    if headers is None:
        return headers
    return {
        key: REDACTED if str(key).lower() == "authorization" else value
        for key, value in headers.items()
    }


def is_oauth_token_endpoint(url: str) -> bool:
    return url.endswith("/oauth/tokens")


def _sanitize_form_body(body: str) -> str:
    sanitized_pairs = [
        (key, REDACTED if key in SENSITIVE_BODY_KEYS else value)
        for key, value in parse_qsl(body, keep_blank_values=True)
    ]
    return urlencode(sanitized_pairs, safe="*")


def sanitize_body(body: Any) -> Any:
    if body is None:
        return body
    if isinstance(body, (dict, list, tuple)):
        return sanitize_data(body)
    if isinstance(body, bytes):
        try:
            return sanitize_body(body.decode("utf-8"))
        except UnicodeDecodeError:
            return REDACTED
    if isinstance(body, str):
        try:
            parsed = json.loads(body)
            return json.dumps(sanitize_data(parsed))
        except json.JSONDecodeError:
            pass
        if "=" in body:
            return _sanitize_form_body(body)
        return body
    return str(body)


def sanitize_body_for_log(body: Any, url: str) -> Any:
    if is_oauth_token_endpoint(url):
        return OAUTH_TOKEN_BODY_REDACTED
    return sanitize_body(body)


def safe_sanitize_headers(headers: Any) -> Any:
    try:
        return sanitize_headers(headers)
    except Exception as e:
        sys.stderr.write(f"Failed to sanitize debug log headers: {e}\n")
        return SANITIZE_FAILURE_MESSAGE


def safe_sanitize_body_for_log(body: Any, url: str) -> Any:
    try:
        return sanitize_body_for_log(body, url)
    except Exception as e:
        sys.stderr.write(f"Failed to sanitize debug log body: {e}\n")
        return SANITIZE_FAILURE_MESSAGE
