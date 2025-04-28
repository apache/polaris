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
# CreateViewRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**location** | **str** |  | [optional] 
**var_schema** | [**ModelSchema**](ModelSchema.md) |  | 
**view_version** | [**ViewVersion**](ViewVersion.md) |  | 
**properties** | **Dict[str, str]** |  | 

## Example

```python
from polaris.catalog.models.create_view_request import CreateViewRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateViewRequest from a JSON string
create_view_request_instance = CreateViewRequest.from_json(json)
# print the JSON string representation of the object
print(CreateViewRequest.to_json())

# convert the object into a dict
create_view_request_dict = create_view_request_instance.to_dict()
# create an instance of CreateViewRequest from a dict
create_view_request_from_dict = CreateViewRequest.from_dict(create_view_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


