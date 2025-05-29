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
# ConnectionConfigInfo

A connection configuration representing a remote catalog service. IMPORTANT - Specifying a ConnectionConfigInfo in an ExternalCatalog is currently an experimental API and is subject to change.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**connection_type** | **str** | The type of remote catalog service represented by this connection | 
**uri** | **str** | URI to the remote catalog service | [optional] 
**authentication_parameters** | [**AuthenticationParameters**](AuthenticationParameters.md) |  | [optional] 
**service_identity** | [**ServiceIdentityInfo**](ServiceIdentityInfo.md) |  | [optional] 

## Example

```python
from polaris.management.models.connection_config_info import ConnectionConfigInfo

# TODO update the JSON string below
json = "{}"
# create an instance of ConnectionConfigInfo from a JSON string
connection_config_info_instance = ConnectionConfigInfo.from_json(json)
# print the JSON string representation of the object
print(ConnectionConfigInfo.to_json())

# convert the object into a dict
connection_config_info_dict = connection_config_info_instance.to_dict()
# create an instance of ConnectionConfigInfo from a dict
connection_config_info_from_dict = ConnectionConfigInfo.from_dict(connection_config_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


