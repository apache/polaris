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

import os.path
import time

import pytest

from integration_tests.conftest import format_namespace
from polaris.catalog import (
    CreateNamespaceRequest,
    CreateTableRequest,
    ModelSchema,
    TableIdentifier,
    IcebergCatalogAPI,
)
from pyiceberg.schema import Schema
from polaris.management import Catalog
from pyiceberg.catalog import Catalog as PyIcebergCatalog


def test_create_namespace(
    test_catalog: Catalog, test_catalog_client: IcebergCatalogAPI
) -> None:
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS1"]),
    )
    resp = test_catalog_client.list_namespaces(test_catalog.name)
    assert resp.namespaces == [["NS1"]]


def test_list_namespaces(
    test_catalog: Catalog, test_catalog_client: IcebergCatalogAPI
) -> None:
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS1"]),
    )
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS1", "NS3"]),
    )
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS2"]),
    )
    assert test_catalog_client.list_namespaces(test_catalog.name).namespaces == [
        ["NS1"],
        ["NS2"],
    ]
    assert test_catalog_client.list_namespaces(
        test_catalog.name, parent="NS1"
    ).namespaces == [["NS1", "NS3"]]


def test_drop_namespace(
    test_catalog: Catalog, test_catalog_client: IcebergCatalogAPI
) -> None:
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS1"]),
    )
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS1", "NS3"]),
    )
    test_catalog_client.drop_namespace(
        prefix=test_catalog.name, namespace=format_namespace(["NS1", "NS3"])
    )
    assert (
        test_catalog_client.list_namespaces(test_catalog.name, parent="NS1").namespaces
        == []
    )


def test_create_table(
    test_catalog: Catalog, test_catalog_client: IcebergCatalogAPI
) -> None:
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS1"]),
    )
    test_table_identifier = TableIdentifier(namespace=["NS1"], name="some_table")
    test_catalog_client.create_table(
        prefix=test_catalog.name,
        namespace="NS1",
        x_iceberg_access_delegation="true",
        create_table_request=CreateTableRequest(
            name="some_table",
            var_schema=ModelSchema(
                type="struct",
                fields=[],
            ),
        ),
    )
    assert test_catalog_client.list_tables(
        prefix=test_catalog.name, namespace="NS1"
    ).identifiers == [test_table_identifier]


def test_load_table(
    test_pyiceberg_catalog: PyIcebergCatalog,
    test_catalog: Catalog,
    test_catalog_client: IcebergCatalogAPI,
    test_table_schema: Schema,
) -> None:
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS1"]),
    )
    tbl = test_pyiceberg_catalog.create_table(
        "NS1.test_table", schema=test_table_schema
    )
    response = test_catalog_client.load_table(
        prefix=test_catalog.name, namespace="NS1", table="test_table"
    )
    assert tbl.metadata_location == response.metadata_location


def test_drop_table_purge(
    test_pyiceberg_catalog: PyIcebergCatalog,
    test_catalog: Catalog,
    test_catalog_client: IcebergCatalogAPI,
    test_table_schema: Schema,
) -> None:
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=["NS1"]),
    )
    tbl = test_pyiceberg_catalog.create_table(
        "NS1.test_table", schema=test_table_schema
    )
    metadata_location = tbl.metadata_location
    test_catalog_client.drop_table(
        prefix=test_catalog.name,
        namespace="NS1",
        table="test_table",
        purge_requested=True,
    )
    assert (
        test_catalog_client.list_tables(
            prefix=test_catalog.name, namespace="NS1"
        ).identifiers
        == []
    )
    attempts = 0

    while os.path.exists(metadata_location) and attempts < 60:
        time.sleep(1)
        attempts += 1

    if os.path.exists(metadata_location):
        pytest.fail(f"Metadata file {metadata_location} still exists after 60 seconds.")
