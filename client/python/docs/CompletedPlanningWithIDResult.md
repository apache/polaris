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
# CompletedPlanningWithIDResult


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**delete_files** | [**List[DeleteFile]**](DeleteFile.md) | Delete files referenced by file scan tasks | [optional] 
**file_scan_tasks** | [**List[FileScanTask]**](FileScanTask.md) |  | [optional] 
**plan_tasks** | **List[str]** |  | [optional] 
**status** | [**PlanStatus**](PlanStatus.md) |  | 
**plan_id** | **str** | ID used to track a planning request | [optional] 

## Example

```python
from polaris.catalog.models.completed_planning_with_id_result import CompletedPlanningWithIDResult

# TODO update the JSON string below
json = "{}"
# create an instance of CompletedPlanningWithIDResult from a JSON string
completed_planning_with_id_result_instance = CompletedPlanningWithIDResult.from_json(json)
# print the JSON string representation of the object
print(CompletedPlanningWithIDResult.to_json())

# convert the object into a dict
completed_planning_with_id_result_dict = completed_planning_with_id_result_instance.to_dict()
# create an instance of CompletedPlanningWithIDResult from a dict
completed_planning_with_id_result_from_dict = CompletedPlanningWithIDResult.from_dict(completed_planning_with_id_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


