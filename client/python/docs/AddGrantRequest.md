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
# AddGrantRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**grant** | [**GrantResource**](GrantResource.md) |  | [optional] 

## Example

```python
from polaris.management.models.add_grant_request import AddGrantRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AddGrantRequest from a JSON string
add_grant_request_instance = AddGrantRequest.from_json(json)
# print the JSON string representation of the object
print(AddGrantRequest.to_json())

# convert the object into a dict
add_grant_request_dict = add_grant_request_instance.to_dict()
# create an instance of AddGrantRequest from a dict
add_grant_request_from_dict = AddGrantRequest.from_dict(add_grant_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


