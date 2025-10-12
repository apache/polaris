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

"""Authorization helpers for the Polaris MCP server."""

from __future__ import annotations

import json
import threading
import time
from abc import ABC, abstractmethod
from typing import Optional
from urllib.parse import urlencode

import urllib3


class AuthorizationProvider(ABC):
    """Return Authorization header values for outgoing requests."""

    @abstractmethod
    def authorization_header(self) -> Optional[str]:
        ...


class StaticAuthorizationProvider(AuthorizationProvider):
    """Wrap a static bearer token."""

    def __init__(self, token: Optional[str]) -> None:
        value = (token or "").strip()
        self._header = f"Bearer {value}" if value else None

    def authorization_header(self) -> Optional[str]:
        return self._header


class ClientCredentialsAuthorizationProvider(AuthorizationProvider):
    """Implements the OAuth client-credentials flow with caching."""

    def __init__(
        self,
        token_endpoint: str,
        client_id: str,
        client_secret: str,
        scope: Optional[str],
        http: urllib3.PoolManager,
    ) -> None:
        self._token_endpoint = token_endpoint
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._http = http
        self._lock = threading.Lock()
        self._cached: Optional[tuple[str, float]] = None  # (token, expires_at_epoch)

    def authorization_header(self) -> Optional[str]:
        token = self._current_token()
        return f"Bearer {token}" if token else None

    def _current_token(self) -> Optional[str]:
        now = time.time()
        cached = self._cached
        if not cached or cached[1] - 60 <= now:
            with self._lock:
                cached = self._cached
                if not cached or cached[1] - 60 <= time.time():
                    self._cached = cached = self._fetch_token()
        return cached[0] if cached else None

    def _fetch_token(self) -> tuple[str, float]:
        payload = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if self._scope:
            payload["scope"] = self._scope

        encoded = urlencode(payload)
        response = self._http.request(
            "POST",
            self._token_endpoint,
            body=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=urllib3.Timeout(connect=20.0, read=20.0),
        )

        if response.status != 200:
            raise RuntimeError(
                f"OAuth token endpoint returned {response.status}: {response.data.decode('utf-8', errors='ignore')}"
            )

        try:
            document = json.loads(response.data.decode("utf-8"))
        except json.JSONDecodeError as error:
            raise RuntimeError("OAuth token endpoint returned invalid JSON") from error

        token = document.get("access_token")
        if not isinstance(token, str) or not token:
            raise RuntimeError("OAuth token response missing access_token")

        expires_in = document.get("expires_in", 3600)
        try:
            ttl = float(expires_in)
        except (TypeError, ValueError):
            ttl = 3600.0
        ttl = max(ttl, 60.0)
        expires_at = time.time() + ttl
        return token, expires_at


class _NoneAuthorizationProvider(AuthorizationProvider):
    def authorization_header(self) -> Optional[str]:
        return None


def none() -> AuthorizationProvider:
    """Return an AuthorizationProvider that never supplies a header."""

    return _NoneAuthorizationProvider()
