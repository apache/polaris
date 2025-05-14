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
# ScanTasks

Scan and planning tasks for server-side scan planning  - `plan-tasks` contains opaque units of planning work - `file-scan-tasks` contains a partial or complete list of table scan tasks - `delete-files` contains delete files referenced by file scan tasks  Each plan task must be passed to the fetchScanTasks endpoint to fetch the file scan tasks for the plan task.  The list of delete files must contain all delete files referenced by the file scan tasks. 

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**delete_files** | [**List[DeleteFile]**](DeleteFile.md) | Delete files referenced by file scan tasks | [optional] 
**file_scan_tasks** | [**List[FileScanTask]**](FileScanTask.md) |  | [optional] 
**plan_tasks** | **List[str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.scan_tasks import ScanTasks

# TODO update the JSON string below
json = "{}"
# create an instance of ScanTasks from a JSON string
scan_tasks_instance = ScanTasks.from_json(json)
# print the JSON string representation of the object
print(ScanTasks.to_json())

# convert the object into a dict
scan_tasks_dict = scan_tasks_instance.to_dict()
# create an instance of ScanTasks from a dict
scan_tasks_from_dict = ScanTasks.from_dict(scan_tasks_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


