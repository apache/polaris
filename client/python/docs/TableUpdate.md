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
# TableUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**format_version** | **int** |  | 
**var_schema** | [**ModelSchema**](ModelSchema.md) |  | 
**last_column_id** | **int** | This optional field is **DEPRECATED for REMOVAL** since it more safe to handle this internally, and shouldn&#39;t be exposed to the clients. The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side. | [optional] 
**schema_id** | **int** | Schema ID to set as current, or -1 to set last added schema | 
**spec** | [**PartitionSpec**](PartitionSpec.md) |  | 
**spec_id** | **int** | Partition spec ID to set as the default, or -1 to set last added spec | 
**sort_order** | [**SortOrder**](SortOrder.md) |  | 
**sort_order_id** | **int** | Sort order ID to set as the default, or -1 to set last added sort order | 
**snapshot** | [**Snapshot**](Snapshot.md) |  | 
**ref_name** | **str** |  | 
**type** | **str** |  | 
**snapshot_id** | **int** |  | 
**max_ref_age_ms** | **int** |  | [optional] 
**max_snapshot_age_ms** | **int** |  | [optional] 
**min_snapshots_to_keep** | **int** |  | [optional] 
**snapshot_ids** | **List[int]** |  | 
**location** | **str** |  | 
**updates** | **Dict[str, str]** |  | 
**removals** | **List[str]** |  | 
**statistics** | [**StatisticsFile**](StatisticsFile.md) |  | 
**spec_ids** | **List[int]** |  | 

## Example

```python
from polaris.catalog.models.table_update import TableUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of TableUpdate from a JSON string
table_update_instance = TableUpdate.from_json(json)
# print the JSON string representation of the object
print(TableUpdate.to_json())

# convert the object into a dict
table_update_dict = table_update_instance.to_dict()
# create an instance of TableUpdate from a dict
table_update_from_dict = TableUpdate.from_dict(table_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


