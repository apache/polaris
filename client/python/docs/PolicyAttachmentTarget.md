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
# PolicyAttachmentTarget


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** | Policy can be attached to different levels: 1. table-like: Policies specific to individual tables or views. 2. namespace: Policies applies to a namespace. 3. catalog: Policies that applies to a catalog  | 
**path** | **List[str]** | A list representing the hierarchical path to the target, ordered from the namespace level down to the entity.  If the target is catalog, the path should be either empty or not set.  | [optional] 

## Example

```python
from polaris.catalog.models.policy_attachment_target import PolicyAttachmentTarget

# TODO update the JSON string below
json = "{}"
# create an instance of PolicyAttachmentTarget from a JSON string
policy_attachment_target_instance = PolicyAttachmentTarget.from_json(json)
# print the JSON string representation of the object
print(PolicyAttachmentTarget.to_json())

# convert the object into a dict
policy_attachment_target_dict = policy_attachment_target_instance.to_dict()
# create an instance of PolicyAttachmentTarget from a dict
policy_attachment_target_from_dict = PolicyAttachmentTarget.from_dict(policy_attachment_target_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


