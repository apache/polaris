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
# AssertCurrentSchemaId

The table's current schema id must match the requirement's `current-schema-id`

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | [optional] 
**current_schema_id** | **int** |  | 

## Example

```python
from polaris.catalog.models.assert_current_schema_id import AssertCurrentSchemaId

# TODO update the JSON string below
json = "{}"
# create an instance of AssertCurrentSchemaId from a JSON string
assert_current_schema_id_instance = AssertCurrentSchemaId.from_json(json)
# print the JSON string representation of the object
print(AssertCurrentSchemaId.to_json())

# convert the object into a dict
assert_current_schema_id_dict = assert_current_schema_id_instance.to_dict()
# create an instance of AssertCurrentSchemaId from a dict
assert_current_schema_id_from_dict = AssertCurrentSchemaId.from_dict(assert_current_schema_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


