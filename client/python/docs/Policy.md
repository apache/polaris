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
# Policy

A policy in Apache Polaris defines a set of rules for governing access, data usage, and operational consistency across various catalog resources.  Policies are stored within Polaris and can be attached to catalogs, namespaces, tables, or views. For example, they can be used for fine-grained control over who can perform specific actions on certain resources.  The policy object includes - **policy-type:** The type of the policy, which determines the expected format and semantics of the policy content. - **inheritable:** A boolean flag indicating whether the policy is inheritable.  - **name:**  A human-readable name for the policy, which must be unique within a given namespace. - **description:** Detailed description of the purpose and functionalities of the policy. - **content:** Policy content, which can be validated against predefined schemas of a policy type. - **version:** Indicates the current version of the policy. Versions increased monotonically, the default value is 0  Policies stored in Polaris serve as the persistent definition for access control and governance rules. 

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**policy_type** | **str** |  | 
**inheritable** | **bool** |  | 
**name** | **str** | A policy name. A valid policy name should only consist of uppercase and lowercase letters (A-Z, a-z), digits (0-9), hyphens (-), underscores (_). | 
**description** | **str** |  | [optional] 
**content** | **str** |  | [optional] 
**version** | **int** |  | 

## Example

```python
from polaris.catalog.models.policy import Policy

# TODO update the JSON string below
json = "{}"
# create an instance of Policy from a JSON string
policy_instance = Policy.from_json(json)
# print the JSON string representation of the object
print(Policy.to_json())

# convert the object into a dict
policy_dict = policy_instance.to_dict()
# create an instance of Policy from a dict
policy_from_dict = Policy.from_dict(policy_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


