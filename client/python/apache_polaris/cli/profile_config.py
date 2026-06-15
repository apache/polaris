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
import stat
import tempfile
from typing import Any, Dict

from apache_polaris.cli.constants import CONFIG_DIR, CONFIG_FILE

CONFIG_FILE_MODE = 0o600


def mask_client_secret(client_secret: str | None) -> str | None:
    if client_secret is None:
        return None
    if len(client_secret) <= 4:
        return "*" * len(client_secret)
    return "*" * (len(client_secret) - 4) + client_secret[-4:]


def format_profile_for_display(profile: dict) -> dict:
    displayed = dict(profile)
    if "client_secret" in displayed:
        displayed["client_secret"] = mask_client_secret(displayed["client_secret"])
    return displayed


def ensure_config_file_permissions(path: str | None = None) -> None:
    if path is None:
        path = CONFIG_FILE
    if not os.path.exists(path):
        return
    current_mode = stat.S_IMODE(os.stat(path).st_mode)
    if current_mode != CONFIG_FILE_MODE:
        os.chmod(path, CONFIG_FILE_MODE)


def load_profiles() -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(CONFIG_FILE):
        return {}
    ensure_config_file_permissions()
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def save_profiles(profiles: Dict[str, Dict[str, Any]]) -> None:
    if not os.path.exists(CONFIG_DIR):
        os.makedirs(CONFIG_DIR)
    temp_path = None
    try:
        fd, temp_path = tempfile.mkstemp(
            dir=CONFIG_DIR, prefix=".polaris.json.", suffix=".tmp"
        )
        with os.fdopen(fd, "w") as f:
            json.dump(profiles, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(temp_path, CONFIG_FILE_MODE)
        os.replace(temp_path, CONFIG_FILE)
        temp_path = None
    finally:
        if temp_path is not None and os.path.exists(temp_path):
            os.unlink(temp_path)
    ensure_config_file_permissions()
