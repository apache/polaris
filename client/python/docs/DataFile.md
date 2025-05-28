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
# DataFile


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**content** | **str** |  | 
**column_sizes** | [**CountMap**](CountMap.md) | Map of column id to total count, including null and NaN | [optional] 
**value_counts** | [**CountMap**](CountMap.md) | Map of column id to null value count | [optional] 
**null_value_counts** | [**CountMap**](CountMap.md) | Map of column id to null value count | [optional] 
**nan_value_counts** | [**CountMap**](CountMap.md) | Map of column id to number of NaN values in the column | [optional] 
**lower_bounds** | [**ValueMap**](ValueMap.md) | Map of column id to lower bound primitive type values | [optional] 
**upper_bounds** | [**ValueMap**](ValueMap.md) | Map of column id to upper bound primitive type values | [optional] 

## Example

```python
from polaris.catalog.models.data_file import DataFile

# TODO update the JSON string below
json = "{}"
# create an instance of DataFile from a JSON string
data_file_instance = DataFile.from_json(json)
# print the JSON string representation of the object
print(DataFile.to_json())

# convert the object into a dict
data_file_dict = data_file_instance.to_dict()
# create an instance of DataFile from a dict
data_file_from_dict = DataFile.from_dict(data_file_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


