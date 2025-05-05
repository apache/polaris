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
# FetchScanTasksRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**plan_task** | **str** | An opaque string provided by the REST server that represents a unit of work to produce file scan tasks for scan planning. This allows clients to fetch tasks across multiple requests to accommodate large result sets. | 

## Example

```python
from polaris.catalog.models.fetch_scan_tasks_request import FetchScanTasksRequest

# TODO update the JSON string below
json = "{}"
# create an instance of FetchScanTasksRequest from a JSON string
fetch_scan_tasks_request_instance = FetchScanTasksRequest.from_json(json)
# print the JSON string representation of the object
print(FetchScanTasksRequest.to_json())

# convert the object into a dict
fetch_scan_tasks_request_dict = fetch_scan_tasks_request_instance.to_dict()
# create an instance of FetchScanTasksRequest from a dict
fetch_scan_tasks_request_from_dict = FetchScanTasksRequest.from_dict(fetch_scan_tasks_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


