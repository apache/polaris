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
# FileScanTask


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**data_file** | [**DataFile**](DataFile.md) |  | 
**delete_file_references** | **List[int]** | A list of indices in the delete files array (0-based) | [optional] 
**residual_filter** | [**Expression**](Expression.md) | An optional filter to be applied to rows in this file scan task. If the residual is not present, the client must produce the residual or use the original filter. | [optional] 

## Example

```python
from polaris.catalog.models.file_scan_task import FileScanTask

# TODO update the JSON string below
json = "{}"
# create an instance of FileScanTask from a JSON string
file_scan_task_instance = FileScanTask.from_json(json)
# print the JSON string representation of the object
print(FileScanTask.to_json())

# convert the object into a dict
file_scan_task_dict = file_scan_task_instance.to_dict()
# create an instance of FileScanTask from a dict
file_scan_task_from_dict = FileScanTask.from_dict(file_scan_task_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


