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
# AssignUUIDUpdate

Assigning a UUID to a table/view should only be done when creating the table/view. It is not safe to re-assign the UUID if a table/view already has a UUID assigned

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | [optional] 
**uuid** | **str** |  | 

## Example

```python
from polaris.catalog.models.assign_uuid_update import AssignUUIDUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of AssignUUIDUpdate from a JSON string
assign_uuid_update_instance = AssignUUIDUpdate.from_json(json)
# print the JSON string representation of the object
print(AssignUUIDUpdate.to_json())

# convert the object into a dict
assign_uuid_update_dict = assign_uuid_update_instance.to_dict()
# create an instance of AssignUUIDUpdate from a dict
assign_uuid_update_from_dict = AssignUUIDUpdate.from_dict(assign_uuid_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


