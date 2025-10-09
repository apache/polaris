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
import re

from polaris.catalog.api_client import ApiClient
from polaris.catalog.configuration import Configuration
from polaris.management import PolarisDefaultApi


def get_catalog_api_client(api: PolarisDefaultApi) -> ApiClient:
    """
    Convert a management API to a catalog API client
    """
    mgmt_config = api.api_client.configuration
    catalog_host = re.sub(r"/api/management(?:/v1)?", "/api/catalog", mgmt_config.host)
    configuration = Configuration(
        host=catalog_host,
        username=mgmt_config.username,
        password=mgmt_config.password,
        access_token=mgmt_config.access_token,
    )

    if hasattr(mgmt_config, "proxy"):
        configuration.proxy = mgmt_config.proxy
    if hasattr(mgmt_config, "proxy_headers"):
        configuration.proxy_headers = mgmt_config.proxy_headers

    return ApiClient(configuration)
