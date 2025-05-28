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
# CreatePolicyRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | A policy name. A valid policy name should only consist of uppercase and lowercase letters (A-Z, a-z), digits (0-9), hyphens (-), underscores (_). | 
**type** | **str** |  | 
**description** | **str** |  | [optional] 
**content** | **str** |  | [optional] 

## Example

```python
from polaris.catalog.models.create_policy_request import CreatePolicyRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreatePolicyRequest from a JSON string
create_policy_request_instance = CreatePolicyRequest.from_json(json)
# print the JSON string representation of the object
print(CreatePolicyRequest.to_json())

# convert the object into a dict
create_policy_request_dict = create_policy_request_instance.to_dict()
# create an instance of CreatePolicyRequest from a dict
create_policy_request_from_dict = CreatePolicyRequest.from_dict(create_policy_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


