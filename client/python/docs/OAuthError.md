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
# OAuthError

The `oauth/tokens` endpoint and related schemas are **DEPRECATED for REMOVAL** from this spec, see description of the endpoint.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**error** | **str** |  | 
**error_description** | **str** |  | [optional] 
**error_uri** | **str** |  | [optional] 

## Example

```python
from polaris.catalog.models.o_auth_error import OAuthError

# TODO update the JSON string below
json = "{}"
# create an instance of OAuthError from a JSON string
o_auth_error_instance = OAuthError.from_json(json)
# print the JSON string representation of the object
print(OAuthError.to_json())

# convert the object into a dict
o_auth_error_dict = o_auth_error_instance.to_dict()
# create an instance of OAuthError from a dict
o_auth_error_from_dict = OAuthError.from_dict(o_auth_error_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


