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
# coding: utf-8

"""
    Apache Iceberg REST Catalog API

    Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.

    The version of the OpenAPI document: 0.0.1
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import pprint
import re  # noqa: F401
import json

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from typing import Any, ClassVar, Dict, List, Optional
from typing_extensions import Annotated
from polaris.catalog.models.metadata_log_inner import MetadataLogInner
from polaris.catalog.models.model_schema import ModelSchema
from polaris.catalog.models.partition_spec import PartitionSpec
from polaris.catalog.models.partition_statistics_file import PartitionStatisticsFile
from polaris.catalog.models.snapshot import Snapshot
from polaris.catalog.models.snapshot_log_inner import SnapshotLogInner
from polaris.catalog.models.snapshot_reference import SnapshotReference
from polaris.catalog.models.sort_order import SortOrder
from polaris.catalog.models.statistics_file import StatisticsFile
from typing import Optional, Set
from typing_extensions import Self

class TableMetadata(BaseModel):
    """
    TableMetadata
    """ # noqa: E501
    format_version: Annotated[int, Field(le=2, strict=True, ge=1)] = Field(alias="format-version")
    table_uuid: StrictStr = Field(alias="table-uuid")
    location: Optional[StrictStr] = None
    last_updated_ms: Optional[StrictInt] = Field(default=None, alias="last-updated-ms")
    properties: Optional[Dict[str, StrictStr]] = None
    schemas: Optional[List[ModelSchema]] = None
    current_schema_id: Optional[StrictInt] = Field(default=None, alias="current-schema-id")
    last_column_id: Optional[StrictInt] = Field(default=None, alias="last-column-id")
    partition_specs: Optional[List[PartitionSpec]] = Field(default=None, alias="partition-specs")
    default_spec_id: Optional[StrictInt] = Field(default=None, alias="default-spec-id")
    last_partition_id: Optional[StrictInt] = Field(default=None, alias="last-partition-id")
    sort_orders: Optional[List[SortOrder]] = Field(default=None, alias="sort-orders")
    default_sort_order_id: Optional[StrictInt] = Field(default=None, alias="default-sort-order-id")
    snapshots: Optional[List[Snapshot]] = None
    refs: Optional[Dict[str, SnapshotReference]] = None
    current_snapshot_id: Optional[StrictInt] = Field(default=None, alias="current-snapshot-id")
    last_sequence_number: Optional[StrictInt] = Field(default=None, alias="last-sequence-number")
    snapshot_log: Optional[List[SnapshotLogInner]] = Field(default=None, alias="snapshot-log")
    metadata_log: Optional[List[MetadataLogInner]] = Field(default=None, alias="metadata-log")
    statistics_files: Optional[List[StatisticsFile]] = Field(default=None, alias="statistics-files")
    partition_statistics_files: Optional[List[PartitionStatisticsFile]] = Field(default=None, alias="partition-statistics-files")
    __properties: ClassVar[List[str]] = ["format-version", "table-uuid", "location", "last-updated-ms", "properties", "schemas", "current-schema-id", "last-column-id", "partition-specs", "default-spec-id", "last-partition-id", "sort-orders", "default-sort-order-id", "snapshots", "refs", "current-snapshot-id", "last-sequence-number", "snapshot-log", "metadata-log", "statistics-files", "partition-statistics-files"]

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        protected_namespaces=(),
    )


    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        # TODO: pydantic v2: use .model_dump_json(by_alias=True, exclude_unset=True) instead
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of TableMetadata from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([
        ])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        # override the default output from pydantic by calling `to_dict()` of each item in schemas (list)
        _items = []
        if self.schemas:
            for _item in self.schemas:
                if _item:
                    _items.append(_item.to_dict())
            _dict['schemas'] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in partition_specs (list)
        _items = []
        if self.partition_specs:
            for _item in self.partition_specs:
                if _item:
                    _items.append(_item.to_dict())
            _dict['partition-specs'] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in sort_orders (list)
        _items = []
        if self.sort_orders:
            for _item in self.sort_orders:
                if _item:
                    _items.append(_item.to_dict())
            _dict['sort-orders'] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in snapshots (list)
        _items = []
        if self.snapshots:
            for _item in self.snapshots:
                if _item:
                    _items.append(_item.to_dict())
            _dict['snapshots'] = _items
        # override the default output from pydantic by calling `to_dict()` of each value in refs (dict)
        _field_dict = {}
        if self.refs:
            for _key in self.refs:
                if self.refs[_key]:
                    _field_dict[_key] = self.refs[_key].to_dict()
            _dict['refs'] = _field_dict
        # override the default output from pydantic by calling `to_dict()` of each item in snapshot_log (list)
        _items = []
        if self.snapshot_log:
            for _item in self.snapshot_log:
                if _item:
                    _items.append(_item.to_dict())
            _dict['snapshot-log'] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in metadata_log (list)
        _items = []
        if self.metadata_log:
            for _item in self.metadata_log:
                if _item:
                    _items.append(_item.to_dict())
            _dict['metadata-log'] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in statistics_files (list)
        _items = []
        if self.statistics_files:
            for _item in self.statistics_files:
                if _item:
                    _items.append(_item.to_dict())
            _dict['statistics-files'] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in partition_statistics_files (list)
        _items = []
        if self.partition_statistics_files:
            for _item in self.partition_statistics_files:
                if _item:
                    _items.append(_item.to_dict())
            _dict['partition-statistics-files'] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of TableMetadata from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "format-version": obj.get("format-version"),
            "table-uuid": obj.get("table-uuid"),
            "location": obj.get("location"),
            "last-updated-ms": obj.get("last-updated-ms"),
            "properties": obj.get("properties"),
            "schemas": [ModelSchema.from_dict(_item) for _item in obj["schemas"]] if obj.get("schemas") is not None else None,
            "current-schema-id": obj.get("current-schema-id"),
            "last-column-id": obj.get("last-column-id"),
            "partition-specs": [PartitionSpec.from_dict(_item) for _item in obj["partition-specs"]] if obj.get("partition-specs") is not None else None,
            "default-spec-id": obj.get("default-spec-id"),
            "last-partition-id": obj.get("last-partition-id"),
            "sort-orders": [SortOrder.from_dict(_item) for _item in obj["sort-orders"]] if obj.get("sort-orders") is not None else None,
            "default-sort-order-id": obj.get("default-sort-order-id"),
            "snapshots": [Snapshot.from_dict(_item) for _item in obj["snapshots"]] if obj.get("snapshots") is not None else None,
            "refs": dict(
                (_k, SnapshotReference.from_dict(_v))
                for _k, _v in obj["refs"].items()
            )
            if obj.get("refs") is not None
            else None,
            "current-snapshot-id": obj.get("current-snapshot-id"),
            "last-sequence-number": obj.get("last-sequence-number"),
            "snapshot-log": [SnapshotLogInner.from_dict(_item) for _item in obj["snapshot-log"]] if obj.get("snapshot-log") is not None else None,
            "metadata-log": [MetadataLogInner.from_dict(_item) for _item in obj["metadata-log"]] if obj.get("metadata-log") is not None else None,
            "statistics-files": [StatisticsFile.from_dict(_item) for _item in obj["statistics-files"]] if obj.get("statistics-files") is not None else None,
            "partition-statistics-files": [PartitionStatisticsFile.from_dict(_item) for _item in obj["partition-statistics-files"]] if obj.get("partition-statistics-files") is not None else None
        })
        return _obj

