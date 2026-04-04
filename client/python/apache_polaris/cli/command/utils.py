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
from typing import List, Optional, Tuple, Deque, Generator, Callable, Any
from collections import deque
from difflib import SequenceMatcher

from apache_polaris.sdk.catalog.api_client import ApiClient
from apache_polaris.sdk.catalog.configuration import Configuration
from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.sdk.catalog import IcebergCatalogAPI
from apache_polaris.cli.constants import UNIT_SEPARATOR, EntityType


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
    Determine if a query matches a target using multi-stage fuzzy strategies and case-insensitive.
    """
    if not query:
        return False
    q = query.lower()
    t = target.lower()
    query_len = len(q)
    # Exact match
    if q == t:
        return True
    # Prefix match
    if t.startswith(q):
        return True
    # Substring match: enabled for length > 1
    if query_len > 1 and q in t:
        return True
    # Similarity: enabled for length > 2
    if query_len > 2:
        return SequenceMatcher(None, q, t).ratio() >= threshold
    return False


def handle_api_exception(entity_label: str, e: Exception) -> None:
    """
    Handle and print API exceptions with formatted output.
    """
    status = getattr(e, "status", None)
    if status == 403:
        print(f"  [x] {entity_label:<30} Permission denied", file=sys.stderr)
    elif status == 404:
        print(f"  [x] {entity_label:<30} Not found", file=sys.stderr)
    elif status:
        print(f"  [x] {entity_label:<30} ERROR (HTTP {status}: {e})", file=sys.stderr)
    else:
        print(f"  [x] {entity_label:<30} Error: {e}", file=sys.stderr)


def format_iceberg_type(obj: Any) -> str:
    """
    Recursively format Iceberg type into a human-readble string.
    """

    def get(o: Any, key: str, default: Any = None) -> Any:
        if isinstance(o, dict):
            return o.get(key, default)
        return getattr(o, key, default)

    unwrapped = get(obj, "actual_instance", obj)
    type_name = get(unwrapped, "type")
    if type_name == "struct":
        fields = get(unwrapped, "fields", [])
        parts = []
        for field in fields:
            name = get(field, "name", "unknown")
            field_type = get(field, "type", "unknown")
            parts.append(f"{name}:{format_iceberg_type(field_type)}")
        return f"struct<{', '.join(parts)}>"
    elif type_name == "list":
        element = get(unwrapped, "element", "unknown")
        return f"list<{format_iceberg_type(element)}>"
    elif type_name == "map":
        key = get(unwrapped, "key", "unknown")
        value = get(unwrapped, "value", "unknown")
        return f"map<{format_iceberg_type(key)}, {format_iceberg_type(value)}>"
    return str(type_name) if isinstance(type_name, str) else str(unwrapped)


def crawl_namespace(
    catalog_api: IcebergCatalogAPI,
    catalog_name: str,
    start_ns: Optional[List[str]] = None,
    on_error: Optional[Callable[[str, Exception], None]] = None,
    entity_type_filter: Optional[str] = None,
) -> Generator[Tuple[EntityType, List[str]], None, None]:
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
        if not entity_type_filter or entity_type_filter == EntityType.NAMESPACE.value:
            yield EntityType.NAMESPACE, current_ns
        # List tables
        if not entity_type_filter or entity_type_filter == EntityType.TABLE.value:
            try:
                resp = catalog_api.list_tables(prefix=catalog_name, namespace=ns_str)
                for table in resp.identifiers or []:
                    # Ensure the listed table is in the same namespace
                    if table.namespace == current_ns:
                        yield EntityType.TABLE, table.namespace + [table.name]
            except Exception as e:
                if on_error:
                    on_error(f"Tables in {catalog_name}.{ns_display}", e)
        # List views:
        if not entity_type_filter or entity_type_filter == EntityType.VIEW.value:
            try:
                resp = catalog_api.list_views(prefix=catalog_name, namespace=ns_str)
                for view in resp.identifiers or []:
                    # Ensure the listed view is in the same namespace
                    if view.namespace == current_ns:
                        yield EntityType.VIEW, view.namespace + [view.name]
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
