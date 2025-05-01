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
# RemovePartitionStatisticsUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | [optional] 
**snapshot_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.remove_partition_statistics_update import RemovePartitionStatisticsUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of RemovePartitionStatisticsUpdate from a JSON string
remove_partition_statistics_update_instance = RemovePartitionStatisticsUpdate.from_json(json)
# print the JSON string representation of the object
print(RemovePartitionStatisticsUpdate.to_json())

# convert the object into a dict
remove_partition_statistics_update_dict = remove_partition_statistics_update_instance.to_dict()
# create an instance of RemovePartitionStatisticsUpdate from a dict
remove_partition_statistics_update_from_dict = RemovePartitionStatisticsUpdate.from_dict(remove_partition_statistics_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


