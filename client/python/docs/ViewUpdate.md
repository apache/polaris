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
# ViewUpdate

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | 
**format_version** | **int** |  | 
**var_schema** | [**ModelSchema**](ModelSchema.md) |  | 
**last_column_id** | **int** | The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side. | [optional] 
**location** | **str** |  | 
**updates** | **Dict[str, str]** |  | 
**removals** | **List[str]** |  | 
**view_version** | [**ViewVersion**](ViewVersion.md) |  | 
**view_version_id** | **int** | The view version id to set as current, or -1 to set last added view version id | 

## Example

```python
from polaris.catalog.models.view_update import ViewUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of ViewUpdate from a JSON string
view_update_instance = ViewUpdate.from_json(json)
# print the JSON string representation of the object
print(ViewUpdate.to_json())

# convert the object into a dict
view_update_dict = view_update_instance.to_dict()
# create an instance of ViewUpdate from a dict
view_update_from_dict = ViewUpdate.from_dict(view_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


