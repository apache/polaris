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
import unittest
from unittest.mock import patch

from apache_polaris.cli.profile_config import (
    CONFIG_FILE_MODE,
    MASKED_CLIENT_SECRET,
    ensure_config_file_permissions,
    format_profile_for_display,
    load_profiles,
    mask_client_secret,
    save_profiles,
)


class TestProfileConfig(unittest.TestCase):
    def test_mask_client_secret_none(self) -> None:
        self.assertIsNone(mask_client_secret(None))

    def test_mask_client_secret_empty(self) -> None:
        self.assertEqual(mask_client_secret(""), "")

    def test_mask_client_secret_non_empty(self) -> None:
        self.assertEqual(mask_client_secret("abcdef123456"), MASKED_CLIENT_SECRET)

    def test_ensure_config_file_permissions_no_op_for_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as config_dir:
            missing_file = os.path.join(config_dir, ".polaris.json")
            ensure_config_file_permissions(missing_file)

    def test_format_profile_for_display_masks_long_secret(self) -> None:
        profile = {
            "client_id": "root",
            "client_secret": "abcdef123456",
            "host": "localhost",
        }
        displayed = format_profile_for_display(profile)
        self.assertEqual(displayed["client_secret"], MASKED_CLIENT_SECRET)
        self.assertEqual(profile["client_secret"], "abcdef123456")

    def test_format_profile_for_display_masks_short_secret(self) -> None:
        profile = {"client_secret": "abcd"}
        displayed = format_profile_for_display(profile)
        self.assertEqual(displayed["client_secret"], MASKED_CLIENT_SECRET)

    def test_format_profile_for_display_preserves_none_secret(self) -> None:
        profile = {"client_id": "root", "client_secret": None}
        displayed = format_profile_for_display(profile)
        self.assertIsNone(displayed["client_secret"])
        self.assertEqual(displayed["client_id"], "root")

    def test_format_profile_for_display_masks_empty_secret(self) -> None:
        profile = {"client_id": "root", "client_secret": ""}
        displayed = format_profile_for_display(profile)
        self.assertEqual(displayed["client_secret"], "")
        self.assertEqual(displayed["client_id"], "root")

    def test_save_profiles_sets_restrictive_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as config_dir:
            config_file = os.path.join(config_dir, ".polaris.json")
            with patch(
                "apache_polaris.cli.profile_config.CONFIG_DIR", config_dir
            ), patch(
                "apache_polaris.cli.profile_config.CONFIG_FILE", config_file
            ):
                save_profiles({"dev": {"client_secret": "secret-value"}})
                mode = stat.S_IMODE(os.stat(config_file).st_mode)
                self.assertEqual(mode, CONFIG_FILE_MODE)

    def test_save_profiles_corrects_existing_permissive_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as config_dir:
            config_file = os.path.join(config_dir, ".polaris.json")
            with open(config_file, "w") as f:
                json.dump({"dev": {"client_secret": "secret-value"}}, f)
            os.chmod(config_file, 0o644)
            with patch(
                "apache_polaris.cli.profile_config.CONFIG_DIR", config_dir
            ), patch(
                "apache_polaris.cli.profile_config.CONFIG_FILE", config_file
            ):
                save_profiles({"dev": {"client_secret": "secret-value"}})
                mode = stat.S_IMODE(os.stat(config_file).st_mode)
                self.assertEqual(mode, CONFIG_FILE_MODE)

    def test_save_profiles_is_atomic_and_readable(self) -> None:
        with tempfile.TemporaryDirectory() as config_dir:
            config_file = os.path.join(config_dir, ".polaris.json")
            profiles = {
                "dev": {
                    "client_id": "root",
                    "client_secret": "s3cr3t",
                    "host": "localhost",
                    "port": 8181,
                }
            }
            with patch(
                "apache_polaris.cli.profile_config.CONFIG_DIR", config_dir
            ), patch(
                "apache_polaris.cli.profile_config.CONFIG_FILE", config_file
            ):
                save_profiles(profiles)
                loaded = load_profiles()
                self.assertEqual(loaded, profiles)
                with open(config_file, "r") as f:
                    on_disk = json.load(f)
                self.assertEqual(on_disk, profiles)
                temp_files = [
                    name
                    for name in os.listdir(config_dir)
                    if name.startswith(".polaris.json.") and name.endswith(".tmp")
                ]
                self.assertEqual(temp_files, [])

    def test_load_profiles_returns_empty_when_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as config_dir:
            config_file = os.path.join(config_dir, ".polaris.json")
            with patch(
                "apache_polaris.cli.profile_config.CONFIG_FILE", config_file
            ):
                self.assertEqual(load_profiles(), {})
