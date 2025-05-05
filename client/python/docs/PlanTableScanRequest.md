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
# PlanTableScanRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**snapshot_id** | **int** | Identifier for the snapshot to scan in a point-in-time scan | [optional] 
**select** | **List[str]** | List of selected schema fields | [optional] 
**filter** | [**Expression**](Expression.md) | Expression used to filter the table data | [optional] 
**case_sensitive** | **bool** | Enables case sensitive field matching for filter and select | [optional] [default to True]
**use_snapshot_schema** | **bool** | Whether to use the schema at the time the snapshot was written. When time travelling, the snapshot schema should be used (true). When scanning a branch, the table schema should be used (false). | [optional] [default to False]
**start_snapshot_id** | **int** | Starting snapshot ID for an incremental scan (exclusive) | [optional] 
**end_snapshot_id** | **int** | Ending snapshot ID for an incremental scan (inclusive). Required when start-snapshot-id is specified. | [optional] 
**stats_fields** | **List[str]** | List of fields for which the service should send column stats. | [optional] 

## Example

```python
from polaris.catalog.models.plan_table_scan_request import PlanTableScanRequest

# TODO update the JSON string below
json = "{}"
# create an instance of PlanTableScanRequest from a JSON string
plan_table_scan_request_instance = PlanTableScanRequest.from_json(json)
# print the JSON string representation of the object
print(PlanTableScanRequest.to_json())

# convert the object into a dict
plan_table_scan_request_dict = plan_table_scan_request_instance.to_dict()
# create an instance of PlanTableScanRequest from a dict
plan_table_scan_request_from_dict = PlanTableScanRequest.from_dict(plan_table_scan_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


