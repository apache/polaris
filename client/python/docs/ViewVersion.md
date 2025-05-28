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
# ViewVersion


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**version_id** | **int** |  | 
**timestamp_ms** | **int** |  | 
**schema_id** | **int** | Schema ID to set as current, or -1 to set last added schema | 
**summary** | **Dict[str, str]** |  | 
**representations** | [**List[ViewRepresentation]**](ViewRepresentation.md) |  | 
**default_catalog** | **str** |  | [optional] 
**default_namespace** | **List[str]** | Reference to one or more levels of a namespace | 

## Example

```python
from polaris.catalog.models.view_version import ViewVersion

# TODO update the JSON string below
json = "{}"
# create an instance of ViewVersion from a JSON string
view_version_instance = ViewVersion.from_json(json)
# print the JSON string representation of the object
print(ViewVersion.to_json())

# convert the object into a dict
view_version_dict = view_version_instance.to_dict()
# create an instance of ViewVersion from a dict
view_version_from_dict = ViewVersion.from_dict(view_version_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


