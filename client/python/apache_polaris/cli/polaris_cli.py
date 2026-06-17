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
import functools
import json
import os
import sys
from json import JSONDecodeError
from typing import Optional, List, Any, cast

import urllib3

from apache_polaris.cli.api_client_builder import ApiClientBuilder
from apache_polaris.cli.command import Command
from apache_polaris.cli.constants import Commands
from apache_polaris.cli.exceptions import CliError, CLI_ERROR_EXIT_CODE
from apache_polaris.cli.options.parser import Parser
from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.sdk.management.exceptions import ApiException
from apache_polaris.cli.command.profiles import ProfilesCommand
from apache_polaris.cli.log_sanitizer import (
    safe_sanitize_body_for_log,
    safe_sanitize_headers,
)


class PolarisCli:
    """
    Implements a basic Command-Line Interface (CLI) for interacting with a Polaris service. The CLI can be used to
    manage entities like catalogs, principals, and grants within Polaris and can perform most operations that are
    available in the Python client API.

    Example usage:
    * polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} principals create example_user
    * polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} principal-roles create example_role
    * polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} catalog-roles list
    * polaris --client-id ${id} --client-secret ${secret} --base-url https://custom-polaris-domain.example.com/service-prefix catalogs list
    """

    # Can be enabled if the client is able to authenticate directly without first fetching a token
    DIRECT_AUTHENTICATION_ENABLED = False

    @staticmethod
    def execute(args: Optional[List[str]] = None) -> None:
        # Print helper message if no argument provided
        if args is None:
            args = sys.argv[1:]
        if not args:
            Parser.build_parser().print_help()
            return
        try:
            options = Parser.parse(args)
            if options.command == Commands.PROFILES:
                command = Command.from_options(options)
                command = cast(ProfilesCommand, command)
                command.execute()
            else:
                api_client = ApiClientBuilder(
                    options,
                    direct_authentication=PolarisCli.DIRECT_AUTHENTICATION_ENABLED,
                ).get_api_client()
                admin_api = PolarisDefaultApi(api_client)
                command = Command.from_options(options)
                if options.debug:
                    PolarisCli._enable_api_request_logging()
                command.execute(admin_api)
        # Handlers from most specific to least: ApiException (OpenAPI client), CliError
        # (expected user/config failures with their own exit codes), NotImplementedError
        # (abstract Command misuse), then generic bugs.
        except ApiException as e:
            PolarisCli.print_api_exception(e)
            sys.exit(CLI_ERROR_EXIT_CODE)
        except CliError as e:
            sys.stderr.write(f"{e}{os.linesep}")
            sys.exit(e.exit_code)
        except NotImplementedError as e:
            sys.stderr.write(f"Internal error: {e}{os.linesep}")
            sys.exit(CLI_ERROR_EXIT_CODE)
        except Exception as e:
            sys.stderr.write(f"An unexpected error occurred: {e}{os.linesep}")
            sys.exit(CLI_ERROR_EXIT_CODE)

    @staticmethod
    def _enable_api_request_logging() -> None:
        # Debug logging mirrors HTTP traffic to stderr. Requests and responses are
        # sanitized before writing so OAuth credentials cannot leak into CI logs or
        # shared terminals (see log_sanitizer.py).
        if not hasattr(urllib3.PoolManager, "original_urlopen"):
            urllib3.PoolManager.original_urlopen = urllib3.PoolManager.urlopen

        @functools.wraps(urllib3.PoolManager.original_urlopen)
        def urlopen_wrapper(
            self: urllib3.PoolManager, method: str, url: str, **kwargs: Any
        ) -> Any:
            sys.stderr.write(f"Request: {method} {url}\n")
            if "headers" in kwargs:
                safe_headers = safe_sanitize_headers(kwargs["headers"])
                sys.stderr.write(f"Headers: {safe_headers}\n")
            if "body" in kwargs:
                safe_body = safe_sanitize_body_for_log(kwargs["body"], url)
                sys.stderr.write(f"Body: {safe_body}\n")

            response = urllib3.PoolManager.original_urlopen(self, method, url, **kwargs)

            sys.stderr.write(f"Response: {response.status}\n")
            safe_response_headers = safe_sanitize_headers(response.headers)
            sys.stderr.write(f"Response Headers: {safe_response_headers}\n")
            response_body = getattr(response, "data", None)
            if response_body:
                safe_response_body = safe_sanitize_body_for_log(response_body, url)
                sys.stderr.write(f"Response Body: {safe_response_body}\n")
            sys.stderr.write("\n")
            return response

        urllib3.PoolManager.urlopen = urlopen_wrapper

    @staticmethod
    def print_api_exception(e: ApiException) -> None:
        try:
            error = json.loads(e.body)["error"]
            sys.stderr.write(
                f"Exception when communicating with the Polaris server."
                f" {error['type']}: {error['message']}{os.linesep}"
            )
        except JSONDecodeError as _:
            sys.stderr.write(
                f"Exception when communicating with the Polaris server."
                f" {e.status}: {e.reason}{os.linesep}"
            )
        except Exception as _:
            sys.stderr.write(
                f"Exception when communicating with the Polaris server. {e}{os.linesep}"
            )


def main() -> None:
    PolarisCli.execute()


if __name__ == "__main__":
    main()
