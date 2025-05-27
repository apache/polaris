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
# AddSchemaUpdate


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**action** | **str** |  | [optional] 
**var_schema** | [**ModelSchema**](ModelSchema.md) |  | 
**last_column_id** | **int** | This optional field is **DEPRECATED for REMOVAL** since it more safe to handle this internally, and shouldn&#39;t be exposed to the clients. The highest assigned column ID for the table. This is used to ensure columns are always assigned an unused ID when evolving schemas. When omitted, it will be computed on the server side. | [optional] 

## Example

```python
from polaris.catalog.models.add_schema_update import AddSchemaUpdate

# TODO update the JSON string below
json = "{}"
# create an instance of AddSchemaUpdate from a JSON string
add_schema_update_instance = AddSchemaUpdate.from_json(json)
# print the JSON string representation of the object
print(AddSchemaUpdate.to_json())

# convert the object into a dict
add_schema_update_dict = add_schema_update_instance.to_dict()
# create an instance of AddSchemaUpdate from a dict
add_schema_update_from_dict = AddSchemaUpdate.from_dict(add_schema_update_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


