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
import logging
from typing import Any
from urllib.parse import parse_qsl, urlencode

logger = logging.getLogger(__name__)

REDACTED = "***REDACTED***"
OAUTH_TOKEN_BODY_REDACTED = "<redacted sensitive authentication payload>"
SANITIZE_FAILURE_MESSAGE = "<redacted: unable to sanitize payload>"

# Compared case-insensitively; ``-`` and ``_`` are treated interchangeably.
SENSITIVE_HEADERS = {"authorization"}
SENSITIVE_BODY_KEYS = {"client_secret", "access_token", "refresh_token"}


def _normalize_key(key: str) -> str:
    return key.lower().replace("-", "_")


def _is_sensitive_body_key(key: str) -> bool:
    return _normalize_key(key) in SENSITIVE_BODY_KEYS


def _should_redact_header(key: str) -> bool:
    return _normalize_key(key) in SENSITIVE_HEADERS


def _redact_body_value(key: str, value: Any) -> Any:
    if _is_sensitive_body_key(key):
        if isinstance(value, (dict, list, tuple)):
            return sanitize_data(value)
        return REDACTED
    return sanitize_data(value)


def sanitize_data(data: Any) -> Any:
    if isinstance(data, dict):
        return {
            key: _redact_body_value(str(key), value) for key, value in data.items()
        }
    if isinstance(data, list):
        return [sanitize_data(item) for item in data]
    if isinstance(data, tuple):
        return tuple(sanitize_data(item) for item in data)
    return data


def _iter_header_items(headers: Any) -> Any:
    if isinstance(headers, dict):
        return headers.items()
    if isinstance(headers, list):
        return headers
    if hasattr(headers, "items"):
        return headers.items()
    return None


def sanitize_headers(headers: Any) -> Any:
    if headers is None:
        return headers
    items = _iter_header_items(headers)
    if items is None:
        return headers
    if isinstance(headers, list):
        return [
            (key, REDACTED if _should_redact_header(str(key)) else value)
            for key, value in items
        ]
    return {
        key: REDACTED if _should_redact_header(str(key)) else value
        for key, value in items
    }


def is_oauth_token_endpoint(url: str) -> bool:
    return "/oauth/tokens" in url


def _sanitize_form_body(body: str) -> str:
    sanitized_pairs = [
        (key, REDACTED if _is_sensitive_body_key(key) else value)
        for key, value in parse_qsl(body, keep_blank_values=True)
    ]
    return urlencode(sanitized_pairs)


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
    except Exception:
        logger.debug("Failed to sanitize debug log headers", exc_info=True)
        return SANITIZE_FAILURE_MESSAGE


def safe_sanitize_body_for_log(body: Any, url: str) -> Any:
    try:
        return sanitize_body_for_log(body, url)
    except Exception:
        logger.debug("Failed to sanitize debug log body", exc_info=True)
        return SANITIZE_FAILURE_MESSAGE
