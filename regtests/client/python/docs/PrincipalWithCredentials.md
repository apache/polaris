<!--

 Copyright (c) 2024 Snowflake Computing Inc.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->
# PrincipalWithCredentials

A user with its client id and secret. This type is returned when a new principal is created or when its credentials are rotated

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**principal** | [**Principal**](Principal.md) |  | 
**credentials** | [**PrincipalWithCredentialsCredentials**](PrincipalWithCredentialsCredentials.md) |  | 

## Example

```python
from polaris.management.models.principal_with_credentials import PrincipalWithCredentials

# TODO update the JSON string below
json = "{}"
# create an instance of PrincipalWithCredentials from a JSON string
principal_with_credentials_instance = PrincipalWithCredentials.from_json(json)
# print the JSON string representation of the object
print(PrincipalWithCredentials.to_json())

# convert the object into a dict
principal_with_credentials_dict = principal_with_credentials_instance.to_dict()
# create an instance of PrincipalWithCredentials from a dict
principal_with_credentials_from_dict = PrincipalWithCredentials.from_dict(principal_with_credentials_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


