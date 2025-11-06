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
import os.path
import pytest
import time
from pyiceberg.catalog import Catalog as PyIcebergCatalog
from pyiceberg.schema import Schema

from apache_polaris.sdk.catalog import (
    CreateNamespaceRequest,
    CreateTableRequest,
    ModelSchema,
    TableIdentifier,
    IcebergCatalogAPI,
)
from apache_polaris.sdk.catalog.api.policy_api import PolicyAPI
from apache_polaris.sdk.catalog.exceptions import NotFoundException
from apache_polaris.sdk.catalog.models.attach_policy_request import AttachPolicyRequest
from apache_polaris.sdk.catalog.models.create_policy_request import CreatePolicyRequest
from apache_polaris.sdk.catalog.models.detach_policy_request import DetachPolicyRequest
from apache_polaris.sdk.catalog.models.policy_attachment_target import (
    PolicyAttachmentTarget,
)
from apache_polaris.sdk.catalog.models.update_policy_request import UpdatePolicyRequest
from apache_polaris.sdk.management import Catalog
from integration_tests.conftest import format_namespace


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


def test_policies(
    test_catalog: Catalog,
    test_catalog_client: IcebergCatalogAPI,
    test_policy_api: PolicyAPI,
    test_table_schema: Schema,
) -> None:
    # Resource identifiers
    namespace_name = "POLICY_NS1"
    sub_namespace = "POLICY_NS2"
    sub_namespace_path = [namespace_name, sub_namespace]
    policy_name = "test_policy"
    table_name = "test_table_for_policy"
    table_path = sub_namespace_path + [table_name]

    policy_content = {
        "version": "2025-02-03",
        "enable": True,
        "config": {"target_file_size_bytes": 134217728},
    }
    policy_type = "system.data-compaction"
    policy_description = "A test policy"

    # Create resources
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=[namespace_name]),
    )
    test_catalog_client.create_namespace(
        prefix=test_catalog.name,
        create_namespace_request=CreateNamespaceRequest(namespace=sub_namespace_path),
    )
    test_catalog_client.create_table(
        prefix=test_catalog.name,
        namespace=format_namespace(sub_namespace_path),
        create_table_request=CreateTableRequest(
            name=table_name,
            var_schema=ModelSchema.from_dict(test_table_schema.model_dump()),
        ),
    )
    test_policy_api.create_policy(
        prefix=test_catalog.name,
        namespace=namespace_name,
        create_policy_request=CreatePolicyRequest(
            name=policy_name,
            type=policy_type,
            description=policy_description,
            content=json.dumps(policy_content),
        ),
    )

    try:
        # GET
        loaded_policy = test_policy_api.load_policy(
            prefix=test_catalog.name, namespace=namespace_name, policy_name=policy_name
        )
        assert loaded_policy.policy.name == policy_name
        assert loaded_policy.policy.policy_type == policy_type
        assert loaded_policy.policy.description == policy_description
        assert json.loads(loaded_policy.policy.content) == policy_content

        # LIST
        policies = test_policy_api.list_policies(
            prefix=test_catalog.name, namespace=namespace_name
        )
        assert len(policies.identifiers) == 1
        assert policies.identifiers[0].name == policy_name
        assert policies.identifiers[0].namespace == [namespace_name]

        # UPDATE
        updated_policy_content = {
            "version": "2025-02-03",
            "enable": False,
            "config": {"target_file_size_bytes": 134217728},
        }
        updated_policy_description = "An updated test policy"
        test_policy_api.update_policy(
            prefix=test_catalog.name,
            namespace=namespace_name,
            policy_name=policy_name,
            update_policy_request=UpdatePolicyRequest(
                description=updated_policy_description,
                content=json.dumps(updated_policy_content),
                current_policy_version=loaded_policy.policy.version,
            ),
        )

        # GET after UPDATE
        updated_policy = test_policy_api.load_policy(
            prefix=test_catalog.name, namespace=namespace_name, policy_name=policy_name
        )
        assert updated_policy.policy.description == updated_policy_description
        assert json.loads(updated_policy.policy.content) == updated_policy_content

        # ATTACH to catalog
        test_policy_api.attach_policy(
            prefix=test_catalog.name,
            namespace=namespace_name,
            policy_name=policy_name,
            attach_policy_request=AttachPolicyRequest(
                target=PolicyAttachmentTarget(type="catalog")
            ),
        )

        # GET APPLICABLE on catalog
        applicable_policies_catalog = test_policy_api.get_applicable_policies(
            prefix=test_catalog.name
        )

        assert len(applicable_policies_catalog.applicable_policies) == 1
        assert applicable_policies_catalog.applicable_policies[0].name == policy_name

        # GET inherited APPLICABLE on table
        applicable_for_table_inherit = test_policy_api.get_applicable_policies(
            prefix=test_catalog.name,
            namespace=format_namespace(sub_namespace_path),
            target_name=table_name,
        )
        assert len(applicable_for_table_inherit.applicable_policies) == 1
        assert applicable_for_table_inherit.applicable_policies[0].name == policy_name
        assert applicable_for_table_inherit.applicable_policies[0].inherited

        # DETACH from catalog
        test_policy_api.detach_policy(
            prefix=test_catalog.name,
            namespace=namespace_name,
            policy_name=policy_name,
            detach_policy_request=DetachPolicyRequest(
                target=PolicyAttachmentTarget(type="catalog")
            ),
        )

        # GET APPLICABLE on catalog after DETACH
        applicable_policies_catalog_after_detach = (
            test_policy_api.get_applicable_policies(prefix=test_catalog.name)
        )

        assert len(applicable_policies_catalog_after_detach.applicable_policies) == 0

        # ATTACH to namespace
        test_policy_api.attach_policy(
            prefix=test_catalog.name,
            namespace=namespace_name,
            policy_name=policy_name,
            attach_policy_request=AttachPolicyRequest(
                target=PolicyAttachmentTarget(type="namespace", path=sub_namespace_path)
            ),
        )

        # GET APPLICABLE on namespace
        applicable_policies_namespace = test_policy_api.get_applicable_policies(
            prefix=test_catalog.name, namespace=format_namespace(sub_namespace_path)
        )
        assert len(applicable_policies_namespace.applicable_policies) == 1
        assert applicable_policies_namespace.applicable_policies[0].name == policy_name

        # DETACH from namespace
        test_policy_api.detach_policy(
            prefix=test_catalog.name,
            namespace=namespace_name,
            policy_name=policy_name,
            detach_policy_request=DetachPolicyRequest(
                target=PolicyAttachmentTarget(type="namespace", path=sub_namespace_path)
            ),
        )

        # GET APPLICABLE on namespace after DETACH
        applicable_policies_namespace_after_detach = (
            test_policy_api.get_applicable_policies(
                prefix=test_catalog.name, namespace=format_namespace(sub_namespace_path)
            )
        )
        assert len(applicable_policies_namespace_after_detach.applicable_policies) == 0

        # ATTACH to table
        test_policy_api.attach_policy(
            prefix=test_catalog.name,
            namespace=namespace_name,
            policy_name=policy_name,
            attach_policy_request=AttachPolicyRequest(
                target=PolicyAttachmentTarget(type="table-like", path=table_path)
            ),
        )

        # GET APPLICABLE on table
        applicable_for_table = test_policy_api.get_applicable_policies(
            prefix=test_catalog.name,
            namespace=format_namespace(sub_namespace_path),
            target_name=table_name,
        )
        assert len(applicable_for_table.applicable_policies) == 1
        assert applicable_for_table.applicable_policies[0].name == policy_name

        # DETACH from table
        test_policy_api.detach_policy(
            prefix=test_catalog.name,
            namespace=namespace_name,
            policy_name=policy_name,
            detach_policy_request=DetachPolicyRequest(
                target=PolicyAttachmentTarget(type="table-like", path=table_path)
            ),
        )

        # GET APPLICABLE on table after DETACH
        applicable_for_table_after_detach = test_policy_api.get_applicable_policies(
            prefix=test_catalog.name,
            namespace=format_namespace(sub_namespace_path),
            target_name=table_name,
        )
        assert len(applicable_for_table_after_detach.applicable_policies) == 0

        # DELETE
        test_policy_api.drop_policy(
            prefix=test_catalog.name,
            namespace=namespace_name,
            policy_name=policy_name,
            detach_all=True,
        )

        # LIST after DELETE
        policies_after_delete = test_policy_api.list_policies(
            prefix=test_catalog.name, namespace=namespace_name
        )
        assert len(policies_after_delete.identifiers) == 0

    finally:
        # Cleanup
        try:
            test_policy_api.drop_policy(
                prefix=test_catalog.name,
                namespace=namespace_name,
                policy_name=policy_name,
                detach_all=True,
            )
        except NotFoundException:
            pass
        try:
            test_catalog_client.drop_table(
                prefix=test_catalog.name,
                namespace=format_namespace(sub_namespace_path),
                table=table_name,
            )
        except NotFoundException:
            pass
        try:
            test_catalog_client.drop_namespace(
                prefix=test_catalog.name, namespace=format_namespace(sub_namespace_path)
            )
        except NotFoundException:
            pass
        try:
            test_catalog_client.drop_namespace(
                prefix=test_catalog.name, namespace=namespace_name
            )
        except NotFoundException:
            pass
