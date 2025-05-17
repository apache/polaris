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
# GrantPrincipalRoleRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**principal_role** | [**PrincipalRole**](PrincipalRole.md) |  | [optional] 

## Example

```python
from polaris.management.models.grant_principal_role_request import GrantPrincipalRoleRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GrantPrincipalRoleRequest from a JSON string
grant_principal_role_request_instance = GrantPrincipalRoleRequest.from_json(json)
# print the JSON string representation of the object
print(GrantPrincipalRoleRequest.to_json())

# convert the object into a dict
grant_principal_role_request_dict = grant_principal_role_request_instance.to_dict()
# create an instance of GrantPrincipalRoleRequest from a dict
grant_principal_role_request_from_dict = GrantPrincipalRoleRequest.from_dict(grant_principal_role_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


