<!--

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

-->
# TableMetadata


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**format_version** | **int** |  | 
**table_uuid** | **str** |  | 
**location** | **str** |  | [optional] 
**last_updated_ms** | **int** |  | [optional] 
**properties** | **Dict[str, str]** |  | [optional] 
**schemas** | [**List[ModelSchema]**](ModelSchema.md) |  | [optional] 
**current_schema_id** | **int** |  | [optional] 
**last_column_id** | **int** |  | [optional] 
**partition_specs** | [**List[PartitionSpec]**](PartitionSpec.md) |  | [optional] 
**default_spec_id** | **int** |  | [optional] 
**last_partition_id** | **int** |  | [optional] 
**sort_orders** | [**List[SortOrder]**](SortOrder.md) |  | [optional] 
**default_sort_order_id** | **int** |  | [optional] 
**snapshots** | [**List[Snapshot]**](Snapshot.md) |  | [optional] 
**refs** | [**Dict[str, SnapshotReference]**](SnapshotReference.md) |  | [optional] 
**current_snapshot_id** | **int** |  | [optional] 
**last_sequence_number** | **int** |  | [optional] 
**snapshot_log** | [**List[SnapshotLogInner]**](SnapshotLogInner.md) |  | [optional] 
**metadata_log** | [**List[MetadataLogInner]**](MetadataLogInner.md) |  | [optional] 
**statistics** | [**List[StatisticsFile]**](StatisticsFile.md) |  | [optional] 
**partition_statistics** | [**List[PartitionStatisticsFile]**](PartitionStatisticsFile.md) |  | [optional] 

## Example

```python
from polaris.catalog.models.table_metadata import TableMetadata

# TODO update the JSON string below
json = "{}"
# create an instance of TableMetadata from a JSON string
table_metadata_instance = TableMetadata.from_json(json)
# print the JSON string representation of the object
print(TableMetadata.to_json())

# convert the object into a dict
table_metadata_dict = table_metadata_instance.to_dict()
# create an instance of TableMetadata from a dict
table_metadata_from_dict = TableMetadata.from_dict(table_metadata_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


