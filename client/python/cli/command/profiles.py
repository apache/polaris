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
import os
import sys
import json
from dataclasses import dataclass
from typing import Dict, Optional, List


from cli.command import Command
from cli.constants import (
    Subcommands,
    DEFAULT_HOSTNAME,
    DEFAULT_PORT,
    CONFIG_DIR,
    CONFIG_FILE,
)
from polaris.management import PolarisDefaultApi


@dataclass
class ProfilesCommand(Command):
    """
    A Command implementation to represent `polaris profiles`. The instance attributes correspond to parameters
    that can be provided to various subcommands, except `profiles_subcommand` which represents the subcommand
    itself.

    Example commands:
        * ./polaris profiles create dev
        * ./polaris profiles delete dev
        * ./polaris profiles update dev
        * ./polaris profiles get dev
        * ./polaris profiles list
    """

    profiles_subcommand: str
    profile_name: str

    def _load_profiles(self) -> Dict[str, Dict[str, str]]:
        if not os.path.exists(CONFIG_FILE):
            return {}
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)

    def _save_profiles(self, profiles: Dict[str, Dict[str, str]]) -> None:
        if not os.path.exists(CONFIG_DIR):
            os.makedirs(CONFIG_DIR)
        with open(CONFIG_FILE, "w") as f:
            json.dump(profiles, f, indent=2)

    def _create_profile(self, name: str) -> None:
        profiles = self._load_profiles()
        if name not in profiles:
            client_id = input("Polaris Client ID: ")
            client_secret = input("Polaris Client Secret: ")
            host = input(f"Polaris Host [{DEFAULT_HOSTNAME}]: ") or DEFAULT_HOSTNAME
            port = input(f"Polaris Port [{DEFAULT_PORT}]: ") or DEFAULT_PORT
            profiles[name] = {
                "client_id": client_id,
                "client_secret": client_secret,
                "host": host,
                "port": port,
            }
            self._save_profiles(profiles)
        else:
            print(f"Profile {name} already exists.")
            sys.exit(1)

    def _get_profile(self, name: str) -> Optional[Dict[str, str]]:
        profiles = self._load_profiles()
        return profiles.get(name)

    def _list_profiles(self) -> List[str]:
        profiles = self._load_profiles()
        return list(profiles.keys())

    def _delete_profile(self, name: str) -> None:
        profiles = self._load_profiles()
        if name in profiles:
            del profiles[name]
            self._save_profiles(profiles)

    def _update_profile(self, name: str) -> None:
        profiles = self._load_profiles()
        if name in profiles:
            current_client_id = profiles[name].get("client_id")
            current_client_secret = profiles[name].get("client_secret")
            current_host = profiles[name].get("host")
            current_port = profiles[name].get("port")

            client_id = (
                input(f"Polaris Client ID [{current_client_id}]: ") or current_client_id
            )
            client_secret = (
                input(f"Polaris Client Secret [{current_client_secret}]: ")
                or current_client_secret
            )
            host = input(f"Polaris Client ID [{current_host}]: ") or current_host
            port = input(f"Polaris Client Secret [{current_port}]: ") or current_port
            profiles[name] = {
                "client_id": client_id,
                "client_secret": client_secret,
                "host": host,
                "port": port,
            }
            self._save_profiles(profiles)
        else:
            print(f"Profile {name} does not exist.")
            sys.exit(1)

    def validate(self):
        pass

    def execute(self, api: Optional[PolarisDefaultApi] = None) -> None:
        if self.profiles_subcommand == Subcommands.CREATE:
            self._create_profile(self.profile_name)
            print(f"Polaris profile {self.profile_name} created successfully.")
        elif self.profiles_subcommand == Subcommands.DELETE:
            self._delete_profile(self.profile_name)
            print(f"Polaris profile {self.profile_name} deleted successfully.")
        elif self.profiles_subcommand == Subcommands.UPDATE:
            self._update_profile(self.profile_name)
            print(f"Polaris profile {self.profile_name} updated successfully.")
        elif self.profiles_subcommand == Subcommands.GET:
            profile = self._get_profile(self.profile_name)
            if profile:
                print(f"Polaris profile {self.profile_name}: {profile}")
            else:
                print(f"Polaris profile {self.profile_name} not found.")
        elif self.profiles_subcommand == Subcommands.LIST:
            profiles = self._list_profiles()
            print("Polaris profiles:")
            for profile in profiles:
                print(f" - {profile}")
        else:
            raise Exception(f"{self.profiles_subcommand} is not supported in the CLI")
