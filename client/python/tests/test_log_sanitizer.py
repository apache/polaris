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

import io
import json
import unittest
from unittest.mock import MagicMock, patch

import urllib3

from apache_polaris.cli.log_sanitizer import (
    OAUTH_TOKEN_BODY_REDACTED,
    REDACTED,
    SANITIZE_FAILURE_MESSAGE,
    sanitize_body,
    sanitize_body_for_log,
    sanitize_data,
    sanitize_headers,
    safe_sanitize_body_for_log,
    safe_sanitize_headers,
)
from apache_polaris.cli.polaris_cli import PolarisCli


class TestLogSanitizer(unittest.TestCase):
    def test_authorization_header_redaction(self) -> None:
        headers = {
            "Authorization": "Bearer secret-token",
            "Content-Type": "application/json",
        }
        sanitized = sanitize_headers(headers)
        self.assertEqual(sanitized["Authorization"], REDACTED)
        self.assertEqual(sanitized["Content-Type"], "application/json")

    def test_realm_header_redaction(self) -> None:
        realm_headers = {
            "Polaris-Realm": "realm-internal",
            "realm": "POLARIS",
            "X-Realm": "tenant-a",
            "realm-id": "realm-123",
            "Accept": "application/json",
        }
        sanitized = sanitize_headers(realm_headers)
        for header_name in ("Polaris-Realm", "realm", "X-Realm", "realm-id"):
            self.assertEqual(sanitized[header_name], REDACTED)
            self.assertNotIn(realm_headers[header_name], str(sanitized))
        self.assertEqual(sanitized["Accept"], "application/json")

    def test_oauth_token_request_payload_redaction(self) -> None:
        body = (
            "grant_type=client_credentials&client_id=my-client&"
            "client_secret=super-secret&scope=PRINCIPAL_ROLE:ALL"
        )
        sanitized = sanitize_body(body)
        self.assertIn("client_id=my-client", sanitized)
        self.assertIn("client_secret=", sanitized)
        self.assertNotIn("super-secret", sanitized)

    def test_oauth_token_response_redaction(self) -> None:
        body = json.dumps(
            {
                "access_token": "oauth-access-token",
                "refresh_token": "oauth-refresh-token",
                "id_token": "oauth-id-token",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )
        sanitized = sanitize_body(body)
        parsed = json.loads(sanitized)
        self.assertEqual(parsed["access_token"], REDACTED)
        self.assertEqual(parsed["refresh_token"], REDACTED)
        self.assertEqual(parsed["id_token"], REDACTED)
        self.assertEqual(parsed["token_type"], "Bearer")
        self.assertEqual(parsed["expires_in"], 3600)

    def test_oauth_token_endpoint_body_fully_redacted(self) -> None:
        body = (
            "grant_type=client_credentials&client_id=my-client&"
            "client_secret=super-secret"
        )
        url = "http://localhost:8080/api/catalog/v1/oauth/tokens"
        self.assertEqual(sanitize_body_for_log(body, url), OAUTH_TOKEN_BODY_REDACTED)

        response_body = json.dumps({"access_token": "oauth-access-token"})
        self.assertEqual(
            sanitize_body_for_log(response_body, url), OAUTH_TOKEN_BODY_REDACTED
        )

    def test_nested_json_structures_redact_secrets(self) -> None:
        payload = {
            "principal": {"name": "alice"},
            "credentials": {
                "client_secret": "nested-secret",
                "tokens": [{"access_token": "token-1", "token_type": "Bearer"}],
            },
        }
        sanitized = sanitize_data(payload)
        self.assertEqual(sanitized["principal"]["name"], "alice")
        self.assertEqual(sanitized["credentials"]["client_secret"], REDACTED)
        self.assertEqual(sanitized["credentials"]["tokens"][0]["access_token"], REDACTED)
        self.assertEqual(
            sanitized["credentials"]["tokens"][0]["token_type"], "Bearer"
        )

    def test_expanded_sensitive_keys(self) -> None:
        payload = {
            "secret": "top-secret",
            "private_key": "-----BEGIN PRIVATE KEY-----",
            "client_assertion": "signed-jwt",
            "session_token": "sess-123",
            "access-key": "AKIAIOSFODNN7EXAMPLE",
            "secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "warehouse": "dev",
        }
        sanitized = sanitize_data(payload)
        self.assertEqual(sanitized["warehouse"], "dev")
        for sensitive_field in (
            "secret",
            "private_key",
            "client_assertion",
            "session_token",
            "access-key",
            "secretKey",
        ):
            self.assertEqual(sanitized[sensitive_field], REDACTED)
            self.assertNotIn(payload[sensitive_field], str(sanitized))

    def test_non_sensitive_fields_remain_visible(self) -> None:
        payload = {
            "client_id": "my-client",
            "warehouse": "dev",
            "client_secret": "secret",
        }
        sanitized = sanitize_data(payload)
        self.assertEqual(sanitized["client_id"], "my-client")
        self.assertEqual(sanitized["warehouse"], "dev")
        self.assertEqual(sanitized["client_secret"], REDACTED)

        headers = {"Accept": "application/json", "User-Agent": "polaris-cli"}
        self.assertEqual(sanitize_headers(headers), headers)

    def test_json_body_redaction(self) -> None:
        body = json.dumps(
            {
                "password": "hunter2",
                "username": "alice",
            }
        )
        sanitized = sanitize_body(body)
        parsed = json.loads(sanitized)
        self.assertEqual(parsed["password"], REDACTED)
        self.assertEqual(parsed["username"], "alice")

    def test_malformed_json_does_not_raise(self) -> None:
        body = "{not-valid-json"
        sanitized = sanitize_body(body)
        self.assertEqual(sanitized, body)

    def test_invalid_utf8_bytes_do_not_raise(self) -> None:
        sanitized = sanitize_body(b"\xff\xfe\xfd")
        self.assertEqual(sanitized, REDACTED)

    def test_unknown_payload_types_are_logged_safely(self) -> None:
        sanitized = sanitize_body(12345)
        self.assertEqual(sanitized, "12345")

    def test_sanitize_failures_return_safe_fallback(self) -> None:
        with patch(
            "apache_polaris.cli.log_sanitizer.sanitize_headers",
            side_effect=RuntimeError("boom"),
        ):
            self.assertEqual(
                safe_sanitize_headers({"Authorization": "secret"}),
                SANITIZE_FAILURE_MESSAGE,
            )

        with patch(
            "apache_polaris.cli.log_sanitizer.sanitize_body_for_log",
            side_effect=RuntimeError("boom"),
        ):
            self.assertEqual(
                safe_sanitize_body_for_log('{"token":"secret"}', "http://example"),
                SANITIZE_FAILURE_MESSAGE,
            )


class TestApiRequestLogging(unittest.TestCase):
    def tearDown(self) -> None:
        if hasattr(urllib3.PoolManager, "original_urlopen"):
            urllib3.PoolManager.urlopen = urllib3.PoolManager.original_urlopen
            delattr(urllib3.PoolManager, "original_urlopen")

    def _capture_debug_output(self, **urlopen_kwargs: object) -> str:
        stderr = io.StringIO()
        pool = urllib3.PoolManager()
        response = MagicMock()
        response.status = 200
        response.headers = urlopen_kwargs.pop(
            "response_headers",
            {"Content-Type": "application/json"},
        )
        response.data = urlopen_kwargs.pop(
            "response_data",
            json.dumps(
                {
                    "access_token": "oauth-access-token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                }
            ).encode(),
        )
        with patch("apache_polaris.cli.polaris_cli.sys.stderr", stderr):
            PolarisCli._enable_api_request_logging()
            with patch.object(
                urllib3.PoolManager,
                "original_urlopen",
                return_value=response,
            ) as mock_urlopen:
                pool.urlopen("POST", **urlopen_kwargs)
                mock_urlopen.assert_called_once()
        return stderr.getvalue()

    def test_debug_logging_redacts_oauth_request_and_response(self) -> None:
        output = self._capture_debug_output(
            url="http://localhost:8080/api/catalog/v1/oauth/tokens",
            headers={
                "Authorization": "Bearer secret-token",
                "Polaris-Realm": "realm-internal",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body=(
                "grant_type=client_credentials&client_id=my-client&"
                "client_secret=super-secret"
            ),
        )

        self.assertIn("Authorization", output)
        self.assertIn("Polaris-Realm", output)
        self.assertNotIn("secret-token", output)
        self.assertNotIn("super-secret", output)
        self.assertNotIn("oauth-access-token", output)
        self.assertIn(OAUTH_TOKEN_BODY_REDACTED, output)
        self.assertIn("Response Body:", output)

    def test_debug_logging_redacts_realm_headers_on_management_request(self) -> None:
        output = self._capture_debug_output(
            url="http://localhost:8181/api/management/v1/catalogs",
            headers={
                "Polaris-Realm": "realm-internal",
                "Authorization": "Bearer secret-token",
                "Accept": "application/json",
            },
            body=json.dumps(
                {
                    "name": "sales",
                    "client_id": "my-client",
                    "client_secret": "super-secret",
                }
            ),
            response_data=json.dumps({"catalogs": [{"name": "sales"}]}).encode(),
        )

        self.assertIn("Polaris-Realm", output)
        self.assertNotIn("realm-internal", output)
        self.assertNotIn("secret-token", output)
        self.assertNotIn("super-secret", output)
        self.assertIn('"name": "sales"', output)
        self.assertIn('"client_id": "my-client"', output)

    def test_debug_logging_survives_sanitizer_failures(self) -> None:
        stderr = io.StringIO()
        pool = urllib3.PoolManager()
        response = MagicMock(status=200, headers={}, data=b"ok")
        with (
            patch("apache_polaris.cli.polaris_cli.sys.stderr", stderr),
            patch(
                "apache_polaris.cli.polaris_cli.safe_sanitize_headers",
                side_effect=[SANITIZE_FAILURE_MESSAGE, SANITIZE_FAILURE_MESSAGE],
            ),
            patch(
                "apache_polaris.cli.polaris_cli.safe_sanitize_body_for_log",
                return_value=SANITIZE_FAILURE_MESSAGE,
            ),
        ):
            PolarisCli._enable_api_request_logging()
            with patch.object(
                urllib3.PoolManager,
                "original_urlopen",
                return_value=response,
            ):
                pool.urlopen(
                    "GET",
                    "http://localhost:8181/api/management/v1/catalogs",
                    headers={"Authorization": "secret"},
                    body='{"token":"secret"}',
                )

        output = stderr.getvalue()
        self.assertIn("Request: GET", output)
        self.assertIn("Response: 200", output)
        self.assertNotIn("secret", output)


if __name__ == "__main__":
    unittest.main()
