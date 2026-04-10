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

import unittest
from unittest.mock import MagicMock

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.setup import SetupCommand
from apache_polaris.cli.options.parser import Parser
from apache_polaris.sdk.management import PolarisDefaultApi


class TestSetupCommand(unittest.TestCase):
    def test_setup_apply_matches_direct_catalog_create_for_optional_s3_fields(
        self,
    ) -> None:
        direct_api = MagicMock(spec=PolarisDefaultApi)
        direct_command = Command.from_options(
            Parser.parse(
                [
                    "catalogs",
                    "create",
                    "catalog_name",
                    "--storage-type",
                    "s3",
                    "--default-base-location",
                    "s3://bucket/catalog_name/",
                    "--allowed-location",
                    "s3://bucket/catalog_name/",
                    "--region",
                    "us-east-1",
                    "--endpoint",
                    "http://localhost:9000/",
                    "--endpoint-internal",
                    "http://rustfs:9000",
                    "--path-style-access",
                    "--no-sts",
                ]
            )
        )
        direct_command.execute(direct_api)
        direct_request = direct_api.create_catalog.call_args.args[0].to_dict()

        setup_api = MagicMock(spec=PolarisDefaultApi)
        setup_api.list_catalogs.return_value.catalogs = []
        setup_command = SetupCommand("apply")
        success = setup_command._create_catalogs(
            setup_api,
            [
                {
                    "name": "catalog_name",
                    "storage_type": "s3",
                    "type": "internal",
                    "default_base_location": "s3://bucket/catalog_name/",
                    "allowed_locations": ["s3://bucket/catalog_name/"],
                    "region": "us-east-1",
                    "endpoint": "http://localhost:9000/",
                    "endpoint_internal": "http://rustfs:9000",
                    "path_style_access": True,
                    "sts_unavailable": True,
                }
            ],
        )

        self.assertTrue(success)
        setup_request = setup_api.create_catalog.call_args.args[0].to_dict()

        self.assertEqual(direct_request, setup_request)
        storage_config = setup_request["catalog"]["storageConfigInfo"]
        self.assertNotIn("roleArn", storage_config)
        self.assertTrue(storage_config["stsUnavailable"])


if __name__ == "__main__":
    unittest.main()
