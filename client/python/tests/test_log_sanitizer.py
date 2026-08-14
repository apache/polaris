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
    is_oauth_token_endpoint,
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

    def test_oauth_token_request_payload_redaction(self) -> None:
        body = (
            "grant_type=client_credentials&client_id=my-client&"
            "client_secret=super-secret&scope=PRINCIPAL_ROLE:ALL"
        )
        sanitized = sanitize_body(body)
        self.assertIn("client_id=my-client", sanitized)
        self.assertIn(f"client_secret={REDACTED}", sanitized)
        self.assertNotIn("super-secret", sanitized)
        self.assertNotIn("%2A%2A%2A", sanitized)

    def test_oauth_token_response_redaction(self) -> None:
        body = json.dumps(
            {
                "access_token": "oauth-access-token",
                "refresh_token": "oauth-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )
        sanitized = sanitize_body(body)
        parsed = json.loads(sanitized)
        self.assertEqual(parsed["access_token"], REDACTED)
        self.assertEqual(parsed["refresh_token"], REDACTED)
        self.assertEqual(parsed["token_type"], "Bearer")
        self.assertEqual(parsed["expires_in"], 3600)

    def test_oauth_token_endpoint_matching(self) -> None:
        self.assertTrue(
            is_oauth_token_endpoint("http://localhost:8080/api/catalog/v1/oauth/tokens")
        )
        self.assertFalse(
            is_oauth_token_endpoint(
                "http://localhost:8080/api/catalog/v1/oauth/tokens/extra"
            )
        )

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

    def test_malformed_json_does_not_raise(self) -> None:
        body = "{not-valid-json"
        sanitized = sanitize_body(body)
        self.assertEqual(sanitized, body)

    def test_credential_key_spellings_are_redacted(self) -> None:
        for key in (
            # OAuth on the management-API camelCase wire
            "clientSecret",
            "accessToken",
            "refreshToken",
            "bearerToken",
            # OAuth on the snake_case wire (token endpoint / Iceberg REST config)
            "client_secret",
            "access_token",
            "refresh_token",
            # Bearer token key in Iceberg REST config
            "token",
            # Generic defense in depth
            "password",
            "secret",
            # Iceberg vended-credential storage keys (bare forms)
            "s3.secret-access-key",
            "s3.session-token",
            "gcs.oauth2.token",
            "adls.sas-token",
        ):
            with self.subTest(key=key):
                self.assertEqual(sanitize_data({key: "s"})[key], REDACTED)

    def test_adls_sas_token_with_suffix_is_redacted(self) -> None:
        # ``adls.sas-token`` is emitted with a hostname or account suffix.
        for key in (
            "adls.sas-token.myaccount.dfs.core.windows.net",
            "adls.sas-token.myaccount",
        ):
            with self.subTest(key=key):
                self.assertEqual(sanitize_data({key: "s"})[key], REDACTED)

    def test_sensitive_key_with_structured_value_is_redacted(self) -> None:
        # A sensitive key with a dict/list/tuple value must be fully redacted;
        # earlier revisions recursed into the value, which left secrets in place.
        payload = {
            "clientSecret": {"v": "leak"},
            "accessToken": ["t1", "t2"],
        }
        sanitized = sanitize_data(payload)
        self.assertEqual(sanitized["clientSecret"], REDACTED)
        self.assertEqual(sanitized["accessToken"], REDACTED)

    def test_non_sensitive_lookalike_keys_are_preserved(self) -> None:
        payload = {
            "tokenType": "Bearer",
            "expiresIn": 3600,
            "clientId": "my-client",
        }
        sanitized = sanitize_data(payload)
        self.assertEqual(sanitized, payload)

    def test_sanitize_failures_return_safe_fallback(self) -> None:
        stderr = io.StringIO()
        with patch("apache_polaris.cli.log_sanitizer.sys.stderr", stderr):
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

        output = stderr.getvalue()
        self.assertIn("Failed to sanitize debug log headers: boom", output)
        self.assertIn("Failed to sanitize debug log body: boom", output)


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
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body=(
                "grant_type=client_credentials&client_id=my-client&"
                "client_secret=super-secret"
            ),
        )

        self.assertIn("Authorization", output)
        self.assertNotIn("secret-token", output)
        self.assertNotIn("super-secret", output)
        self.assertNotIn("oauth-access-token", output)
        self.assertIn(OAUTH_TOKEN_BODY_REDACTED, output)
        self.assertIn("Response Body:", output)

    def test_debug_logging_redacts_management_request_credentials(self) -> None:
        output = self._capture_debug_output(
            url="http://localhost:8181/api/management/v1/catalogs",
            headers={
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

        self.assertNotIn("secret-token", output)
        self.assertNotIn("super-secret", output)
        self.assertIn('"name": "sales"', output)
        self.assertIn('"client_id": "my-client"', output)

    def test_debug_logging_redacts_camelcase_credentials_on_the_wire(self) -> None:
        response_body = json.dumps(
            {
                "principal": {"name": "alice", "clientId": "abc"},
                "credentials": {"clientId": "abc", "clientSecret": "hunter2"},
            }
        ).encode()
        output = self._capture_debug_output(
            url="http://localhost:8181/api/management/v1/principals",
            headers={"Authorization": "Bearer admin-token"},
            body=json.dumps(
                {
                    "name": "ext",
                    "connectionConfigInfo": {
                        "authenticationParameters": {
                            "clientId": "id",
                            "clientSecret": "topsecret",
                            "bearerToken": "btok",
                        }
                    },
                }
            ),
            response_data=response_body,
        )

        self.assertNotIn("admin-token", output)
        self.assertNotIn("topsecret", output)
        self.assertNotIn("btok", output)
        self.assertNotIn("hunter2", output)
        self.assertIn('"name": "ext"', output)
        self.assertIn('"clientId": "id"', output)
        self.assertIn('"clientId": "abc"', output)

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
