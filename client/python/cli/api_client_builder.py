#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#
import json
import os
from argparse import Namespace
from functools import cached_property
from typing import Optional, Dict

from cli.constants import (
  CONFIG_FILE,
  CLIENT_PROFILE_ENV,
  CLIENT_ID_ENV,
  CLIENT_SECRET_ENV,
  REALM_ENV,
  HEADER_ENV,
  Arguments,
  DEFAULT_HOSTNAME,
  DEFAULT_PORT
)
from cli.options.option_tree import Argument
from polaris.management import ApiClient, Configuration


def _load_profiles() -> Dict[str, str]:
    if not os.path.exists(CONFIG_FILE):
        return {}
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


class BuilderConfig:
    def __init__(self, options: Namespace):
        self.options = options
        self.proxy = options.proxy
        self.access_token = options.access_token

    @cached_property
    def profile(self) -> Dict[str, str]:
        profile = {}
        client_profile = self.options.profile or os.getenv(CLIENT_PROFILE_ENV)
        if client_profile:
            profiles = _load_profiles()
            profile = profiles.get(client_profile)
            if not profile:
                raise Exception(f"Polaris profile {client_profile} not found")
        return profile

    @cached_property
    def base_url(self) -> str:
        if self.options.base_url:
            if self.options.host is not None or self.options.port is not None:
                raise Exception(
                    f"Please provide either {Argument.to_flag_name(Arguments.BASE_URL)} or"
                    f" {Argument.to_flag_name(Arguments.HOST)} &"
                    f" {Argument.to_flag_name(Arguments.PORT)}, but not both"
                )
            return self.options.base_url

        host = self.options.host or self.profile.get(Arguments.HOST) or DEFAULT_HOSTNAME
        port = self.options.port or self.profile.get(Arguments.PORT) or DEFAULT_PORT
        return f"http://{host}:{port}"

    @cached_property
    def management_url(self) -> str:
        return f"{self.base_url}/api/management/v1"

    @cached_property
    def catalog_url(self) -> str:
        return f"{self.base_url}/api/catalog/v1"

    @cached_property
    def client_id(self) -> Optional[str]:
        return (
                self.options.client_id
                or os.getenv(CLIENT_ID_ENV)
                or self.profile.get(Arguments.CLIENT_ID)
        )

    @cached_property
    def client_secret(self) -> Optional[str]:
        return (
                self.options.client_secret
                or os.getenv(CLIENT_SECRET_ENV)
                or self.profile.get(Arguments.CLIENT_SECRET)
        )

    @cached_property
    def realm(self) -> Optional[str]:
        realms = (
                self.options.realm
                or os.getenv(REALM_ENV)
                or self.profile.get(Arguments.REALM)
        )
        return realms

    @cached_property
    def header(self) -> Optional[str]:
        return (
                self.options.header
                or os.getenv(HEADER_ENV)
                or self.profile.get(Arguments.HEADER)
        )


class ApiClientBuilder:
    def __init__(self, options: Namespace, *, direct_authentication: bool = False):
        self.conf = BuilderConfig(options)
        self.direct_auth_enabled = direct_authentication

    def _get_token(self):
        header_params = {"Content-Type": "application/x-www-form-urlencoded"}
        if self.conf.header and self.conf.realm:
            header_params[self.conf.header] = self.conf.realm

        conf = Configuration(host=self.conf.management_url)
        conf.proxy = self.conf.proxy
        api_client = ApiClient(conf)
        response = api_client.call_api(
            "POST",
            f"{self.conf.catalog_url}/oauth/tokens",
            header_params=header_params,
            post_params={
                "grant_type": "client_credentials",
                "client_id": self.conf.client_id,
                "client_secret": self.conf.client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        ).response.data
        if "access_token" not in json.loads(response):
            raise Exception("Failed to get access token")
        return json.loads(response)["access_token"]

    def _build(self) -> ApiClient:
        has_access_token = self.conf.access_token is not None
        has_client_secret = self.conf.client_id is not None and self.conf.client_secret is not None
        if has_access_token and (self.conf.client_id or self.conf.client_secret):
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

        config = Configuration(host=self.conf.management_url)
        config.proxy = self.conf.proxy
        if has_access_token:
            config.access_token = self.conf.access_token
        elif has_client_secret:
            config.username = self.conf.client_id
            config.password = self.conf.client_secret

        if not has_access_token and not self.direct_auth_enabled:
            config.username = None
            config.password = None
            config.access_token = self._get_token()

        client_params = {}
        if self.conf.realm and self.conf.header:
            client_params["header_name"] = self.conf.header
            client_params["header_value"] = self.conf.realm

        return ApiClient(config, **client_params)

    def get_api_client(self) -> ApiClient:
        return self._build()
