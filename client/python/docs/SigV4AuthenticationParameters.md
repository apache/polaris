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
# SigV4AuthenticationParameters

AWS Signature Version 4 authentication

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**role_arn** | **str** | The aws IAM role arn assumed by polaris userArn when signing requests | 
**role_session_name** | **str** | The role session name to be used by the SigV4 protocol for signing requests | [optional] 
**external_id** | **str** | An optional external id used to establish a trust relationship with AWS in the trust policy | [optional] 
**signing_region** | **str** | Region to be used by the SigV4 protocol for signing requests | 
**signing_name** | **str** | The service name to be used by the SigV4 protocol for signing requests, the default signing name is \&quot;execute-api\&quot; is if not provided | [optional] 

## Example

```python
from polaris.management.models.sig_v4_authentication_parameters import SigV4AuthenticationParameters

# TODO update the JSON string below
json = "{}"
# create an instance of SigV4AuthenticationParameters from a JSON string
sig_v4_authentication_parameters_instance = SigV4AuthenticationParameters.from_json(json)
# print the JSON string representation of the object
print(SigV4AuthenticationParameters.to_json())

# convert the object into a dict
sig_v4_authentication_parameters_dict = sig_v4_authentication_parameters_instance.to_dict()
# create an instance of SigV4AuthenticationParameters from a dict
sig_v4_authentication_parameters_from_dict = SigV4AuthenticationParameters.from_dict(sig_v4_authentication_parameters_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


