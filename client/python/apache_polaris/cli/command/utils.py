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
import sys
import datetime
from typing import List, Optional, Tuple, Deque, Generator, Callable
from collections import deque
from difflib import SequenceMatcher

from apache_polaris.sdk.catalog.api_client import ApiClient
from apache_polaris.sdk.catalog.configuration import Configuration
from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.sdk.catalog import IcebergCatalogAPI
from apache_polaris.cli.constants import UNIT_SEPARATOR


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

    catalog_client = ApiClient(configuration)

    # Preserve custom headers (like Polaris-Realm) from management client
    if hasattr(api.api_client, "default_headers"):
        for header_name, header_value in api.api_client.default_headers.items():
            if header_name != "User-Agent":  # Don't override User-Agent
                catalog_client.set_default_header(header_name, header_value)

    return catalog_client


def format_timestamp(ms_since_epoch: int) -> str:
    """
    Convert a timestamp in milliseconds since epoch to a human-readable string
    """
    if ms_since_epoch is None:
        return "Unknown"
    dt = datetime.datetime.fromtimestamp(
        ms_since_epoch / 1000, tz=datetime.timezone.utc
    )
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def is_fuzzy_match(query: str, target: str, threshold: float = 0.85) -> bool:
    """
    Determine if a query matches a target using multi-stage fuzzy strategies.
    """
    # Substring match
    if query in target:
        return True
    # Spare match
    iterator = iter(target)
    if all(char in iterator for char in query):
        return True
    # Similarity
    if len(query) > 3:
        return SequenceMatcher(None, query, target).ratio() >= threshold
    return False


def handle_api_exception(entity_label: str, e: Exception) -> None:
    """
    Handle and print API exceptions with formatted output.
    """
    status = getattr(e, "status", None)
    if status == 403:
        print(f"  [!] {entity_label:<30} Permission denied", file=sys.stderr)
    elif status:
        print(f"  [!] {entity_label:<30} ERROR (HTTP {status}: {e})", file=sys.stderr)
    else:
        print(f"  [!] {entity_label:<30} Error: {e}", file=sys.stderr)


def resolve_identifier(identifier: str) -> Tuple[Optional[str], List[str], str]:
    """
    Resolve a dot-delimited identifier into (catalog, ns_list, leaf_name)
    """
    if not identifier:
        return None, [], ""
    parts = identifier.split(".")
    # Only one part, it must be table/view
    if len(parts) == 1:
        return None, [], parts[0]
    # Only two parts, must be ns_list and leaf_name
    if len(parts) == 2:
        return None, [parts[0]], parts[1]
    return parts[0], parts[1:-1], parts[-1]


def crawl_namespace(
    catalog_api: IcebergCatalogAPI,
    catalog_name: str,
    start_ns: Optional[List[str]] = None,
    on_error: Optional[Callable[[str, Exception], None]] = None,
) -> Generator[Tuple[str, List[str]], None, None]:
    """
    Iterator BFS to crawl namespaces into (type, path_list)
    """
    visited = set()
    queue: Deque[List[str]] = deque()
    if start_ns:
        queue.append(start_ns)
    else:
        try:
            resp = catalog_api.list_namespaces(prefix=catalog_name)
            for ns in resp.namespaces or []:
                queue.append(ns)
        except Exception as e:
            if on_error:
                on_error(f"Root Namespace ({catalog_name})", e)
    while queue:
        current_ns = queue.popleft()
        ns_str = UNIT_SEPARATOR.join(current_ns)
        ns_display = ".".join(current_ns)
        if ns_str in visited:
            continue
        visited.add(ns_str)
        yield "namespace", current_ns
        # List tables
        try:
            resp = catalog_api.list_tables(prefix=catalog_name, namespace=ns_str)
            for table in resp.identifiers or []:
                # Ensure the listed table is in the same namespace
                if table.namespace == current_ns:
                    yield "table", table.namespace + [table.name]
        except Exception as e:
            if on_error:
                on_error(f"Tables in {catalog_name}.{ns_display}", e)
        # List views:
        try:
            resp = catalog_api.list_views(prefix=catalog_name, namespace=ns_str)
            for view in resp.identifiers or []:
                # Ensure the listed view is in the same namespace
                if view.namespace == current_ns:
                    yield "view", view.namespace + [view.name]
        except Exception as e:
            if on_error:
                on_error(f"Views in {catalog_name}.{ns_display}", e)
        # List sub-namespaces
        try:
            resp = catalog_api.list_namespaces(prefix=catalog_name, parent=ns_str)
            for ns in resp.namespaces or []:
                queue.append(ns)
        except Exception as e:
            if on_error:
                on_error(f"Sub-namespaces of {catalog_name}.{ns_display}", e)
