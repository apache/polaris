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
# GetNamespaceResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**namespace** | **List[str]** | Reference to one or more levels of a namespace | 
**properties** | **Dict[str, str]** | Properties stored on the namespace, if supported by the server. If the server does not support namespace properties, it should return null for this field. If namespace properties are supported, but none are set, it should return an empty object. | [optional] 

## Example

```python
from polaris.catalog.models.get_namespace_response import GetNamespaceResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetNamespaceResponse from a JSON string
get_namespace_response_instance = GetNamespaceResponse.from_json(json)
# print the JSON string representation of the object
print(GetNamespaceResponse.to_json())

# convert the object into a dict
get_namespace_response_dict = get_namespace_response_instance.to_dict()
# create an instance of GetNamespaceResponse from a dict
get_namespace_response_from_dict = GetNamespaceResponse.from_dict(get_namespace_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


