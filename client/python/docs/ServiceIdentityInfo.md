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
# ServiceIdentityInfo

Identity metadata for the Polaris service used to access external resources.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**identity_type** | **str** | The type of identity used to access external resources | 

## Example

```python
from polaris.management.models.service_identity_info import ServiceIdentityInfo

# TODO update the JSON string below
json = "{}"
# create an instance of ServiceIdentityInfo from a JSON string
service_identity_info_instance = ServiceIdentityInfo.from_json(json)
# print the JSON string representation of the object
print(ServiceIdentityInfo.to_json())

# convert the object into a dict
service_identity_info_dict = service_identity_info_instance.to_dict()
# create an instance of ServiceIdentityInfo from a dict
service_identity_info_from_dict = ServiceIdentityInfo.from_dict(service_identity_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


