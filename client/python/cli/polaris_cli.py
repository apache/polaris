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

import json
import os
import sys
from json import JSONDecodeError

from typing import Dict

from cli.constants import (
    Arguments,
    Commands,
    CLIENT_ID_ENV,
    CLIENT_SECRET_ENV,
    CLIENT_PROFILE_ENV,
    DEFAULT_HOSTNAME,
    DEFAULT_PORT,
    CONFIG_FILE,
)
from cli.options.option_tree import Argument
from cli.options.parser import Parser
from polaris.management import ApiClient, Configuration
from polaris.management import PolarisDefaultApi


class PolarisCli:
    """
    Implements a basic Command-Line Interface (CLI) for interacting with a Polaris service. The CLI can be used to
    manage entities like catalogs, principals, and grants within Polaris and can perform most operations that are
    available in the Python client API.

    Example usage:
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} principals create example_user
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} principal-roles create example_role
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} catalog-roles list
    * ./polaris --client-id ${id} --client-secret ${secret} --base-url https://custom-polaris-domain.example.com/service-prefix catalogs list
    """

    # Can be enabled if the client is able to authenticate directly without first fetching a token
    DIRECT_AUTHENTICATION_ENABLED = False

    @staticmethod
    def execute(args=None):
        options = Parser.parse(args)
        if options.command == Commands.PROFILES:
            from cli.command import Command

            command = Command.from_options(options)
            command.execute()
        else:
            client_builder = PolarisCli._get_client_builder(options)
            with client_builder() as api_client:
                try:
                    from cli.command import Command

                    admin_api = PolarisDefaultApi(api_client)
                    command = Command.from_options(options)
                    command.execute(admin_api)
                except Exception as e:
                    PolarisCli._try_print_exception(e)
                    sys.exit(1)

    @staticmethod
    def _try_print_exception(e):
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

    @staticmethod
    def _load_profiles() -> Dict[str, Dict[str, str]]:
        if not os.path.exists(CONFIG_FILE):
            return {}
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)

    @staticmethod
    def _get_token(api_client: ApiClient, catalog_url, client_id, client_secret) -> str:
        response = api_client.call_api(
            "POST",
            f"{catalog_url}/oauth/tokens",
            header_params={"Content-Type": "application/x-www-form-urlencoded"},
            post_params={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        ).response.data
        if "access_token" not in json.loads(response):
            raise Exception("Failed to get access token")
        return json.loads(response)["access_token"]

    @staticmethod
    def _get_client_builder(options):
        profile = {}
        client_profile = options.profile or os.getenv(CLIENT_PROFILE_ENV)
        if client_profile:
            profiles = PolarisCli._load_profiles()
            profile = profiles.get(client_profile)
            if not profile:
                raise Exception(f"Polaris profile {client_profile} not found")
        # Determine which credentials to use
        client_id = (
            options.client_id or os.getenv(CLIENT_ID_ENV) or profile.get("client_id")
        )
        client_secret = (
            options.client_secret
            or os.getenv(CLIENT_SECRET_ENV)
            or profile.get("client_secret")
        )

        # Validates
        has_access_token = options.access_token is not None
        has_client_secret = client_id is not None and client_secret is not None
        if has_access_token and (options.client_id or options.client_secret):
            raise Exception(
                f"Please provide credentials via either {Argument.to_flag_name(Arguments.CLIENT_ID)} &"
                f" {Argument.to_flag_name(Arguments.CLIENT_SECRET)} or"
                f" {Argument.to_flag_name(Arguments.ACCESS_TOKEN)}, but not both"
            )
        if not has_access_token and not has_client_secret:
            raise Exception(
                f"Please provide credentials via either {Argument.to_flag_name(Arguments.CLIENT_ID)} &"
                f" {Argument.to_flag_name(Arguments.CLIENT_SECRET)} or"
                f" {Argument.to_flag_name(Arguments.ACCESS_TOKEN)}."
                f" Alternatively, you may set the environment variables {CLIENT_ID_ENV} &"
                f" {CLIENT_SECRET_ENV}."
            )
        # Authenticate accordingly
        if options.base_url:
            if options.host is not None or options.port is not None:
                raise Exception(
                    f"Please provide either {Argument.to_flag_name(Arguments.BASE_URL)} or"
                    f" {Argument.to_flag_name(Arguments.HOST)} &"
                    f" {Argument.to_flag_name(Arguments.PORT)}, but not both"
                )

            polaris_management_url = f"{options.base_url}/api/management/v1"
            polaris_catalog_url = f"{options.base_url}/api/catalog/v1"
        else:
            host = options.host or profile.get("host") or DEFAULT_HOSTNAME
            port = options.port or profile.get("port") or DEFAULT_PORT
            polaris_management_url = f"http://{host}:{port}/api/management/v1"
            polaris_catalog_url = f"http://{host}:{port}/api/catalog/v1"

        config = Configuration(host=polaris_management_url)
        config.proxy = options.proxy
        if has_access_token:
            config.access_token = options.access_token
        elif has_client_secret:
            config.username = client_id
            config.password = client_secret

        if not has_access_token and not PolarisCli.DIRECT_AUTHENTICATION_ENABLED:
            token = PolarisCli._get_token(
                ApiClient(config), polaris_catalog_url, client_id, client_secret
            )
            config.username = None
            config.password = None
            config.access_token = token

        return lambda: ApiClient(config)


if __name__ == "__main__":
    PolarisCli.execute()
