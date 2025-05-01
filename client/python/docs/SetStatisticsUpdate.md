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
# SetStatisticsUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | [optional] 
**snapshot_id** | **int** | This optional field is **DEPRECATED for REMOVAL** since it contains redundant information. Clients should use the &#x60;statistics.snapshot-id&#x60; field instead. | [optional] 
**statistics** | [**StatisticsFile**](StatisticsFile.md) |  | 

## Example

```python
from polaris.catalog.models.set_statistics_update import SetStatisticsUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of SetStatisticsUpdate from a JSON string
set_statistics_update_instance = SetStatisticsUpdate.from_json(json)
# print the JSON string representation of the object
print(SetStatisticsUpdate.to_json())

# convert the object into a dict
set_statistics_update_dict = set_statistics_update_instance.to_dict()
# create an instance of SetStatisticsUpdate from a dict
set_statistics_update_from_dict = SetStatisticsUpdate.from_dict(set_statistics_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


