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
# LoadViewResult

Result used when a view is successfully loaded.   The view metadata JSON is returned in the `metadata` field. The corresponding file location of view metadata is returned in the `metadata-location` field. Clients can check whether metadata has changed by comparing metadata locations after the view has been created.  The `config` map returns view-specific configuration for the view's resources.  The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for view requests if OAuth2 security is enabled 

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**metadata_location** | **str** |  | 
**metadata** | [**ViewMetadata**](ViewMetadata.md) |  | 
**config** | **Dict[str, str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.load_view_result import LoadViewResult

# TODO update the JSON string below
json = "{}"
# create an instance of LoadViewResult from a JSON string
load_view_result_instance = LoadViewResult.from_json(json)
# print the JSON string representation of the object
print(LoadViewResult.to_json())

# convert the object into a dict
load_view_result_dict = load_view_result_instance.to_dict()
# create an instance of LoadViewResult from a dict
load_view_result_from_dict = LoadViewResult.from_dict(load_view_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


