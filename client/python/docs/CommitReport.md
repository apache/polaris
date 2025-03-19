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
# CommitReport

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table_name** | **str** |  | 
**snapshot_id** | **int** |  | 
**sequence_number** | **int** |  | 
**operation** | **str** |  | 
**metrics** | [**Dict[str, MetricResult]**](MetricResult.md) |  | 
**metadata** | **Dict[str, str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.commit_report import CommitReport

# TODO update the JSON string below
json = "{}"
# create an instance of CommitReport from a JSON string
commit_report_instance = CommitReport.from_json(json)
# print the JSON string representation of the object
print(CommitReport.to_json())

# convert the object into a dict
commit_report_dict = commit_report_instance.to_dict()
# create an instance of CommitReport from a dict
commit_report_from_dict = CommitReport.from_dict(commit_report_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


