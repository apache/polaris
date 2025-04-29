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
# ViewMetadata


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**view_uuid** | **str** |  | 
**format_version** | **int** |  | 
**location** | **str** |  | 
**current_version_id** | **int** |  | 
**versions** | [**List[ViewVersion]**](ViewVersion.md) |  | 
**version_log** | [**List[ViewHistoryEntry]**](ViewHistoryEntry.md) |  | 
**schemas** | [**List[ModelSchema]**](ModelSchema.md) |  | 
**properties** | **Dict[str, str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.view_metadata import ViewMetadata

# TODO update the JSON string below
json = "{}"
# create an instance of ViewMetadata from a JSON string
view_metadata_instance = ViewMetadata.from_json(json)
# print the JSON string representation of the object
print(ViewMetadata.to_json())

# convert the object into a dict
view_metadata_dict = view_metadata_instance.to_dict()
# create an instance of ViewMetadata from a dict
view_metadata_from_dict = ViewMetadata.from_dict(view_metadata_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


