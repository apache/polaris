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
# ReportMetricsRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**report_type** | **str** |  | 
**table_name** | **str** |  | 
**snapshot_id** | **int** |  | 
**filter** | [**Expression**](Expression.md) |  | 
**schema_id** | **int** |  | 
**projected_field_ids** | **List[int]** |  | 
**projected_field_names** | **List[str]** |  | 
**metrics** | [**Dict[str, MetricResult]**](MetricResult.md) |  | 
**metadata** | **Dict[str, str]** |  | [optional] 
**sequence_number** | **int** |  | 
**operation** | **str** |  | 

## Example

```python
from polaris.catalog.models.report_metrics_request import ReportMetricsRequest

# TODO update the JSON string below
json = "{}"
# create an instance of ReportMetricsRequest from a JSON string
report_metrics_request_instance = ReportMetricsRequest.from_json(json)
# print the JSON string representation of the object
print(ReportMetricsRequest.to_json())

# convert the object into a dict
report_metrics_request_dict = report_metrics_request_instance.to_dict()
# create an instance of ReportMetricsRequest from a dict
report_metrics_request_from_dict = ReportMetricsRequest.from_dict(report_metrics_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


