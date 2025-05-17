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
# MetricResult


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**unit** | **str** |  | 
**value** | **int** |  | 
**time_unit** | **str** |  | 
**count** | **int** |  | 
**total_duration** | **int** |  | 

## Example

```python
from polaris.catalog.models.metric_result import MetricResult

# TODO update the JSON string below
json = "{}"
# create an instance of MetricResult from a JSON string
metric_result_instance = MetricResult.from_json(json)
# print the JSON string representation of the object
print(MetricResult.to_json())

# convert the object into a dict
metric_result_dict = metric_result_instance.to_dict()
# create an instance of MetricResult from a dict
metric_result_from_dict = MetricResult.from_dict(metric_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


