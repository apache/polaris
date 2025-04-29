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
# CatalogRole


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | The name of the role | 
**properties** | **Dict[str, str]** |  | [optional] 
**create_timestamp** | **int** |  | [optional] 
**last_update_timestamp** | **int** |  | [optional] 
**entity_version** | **int** | The version of the catalog role object used to determine if the catalog role metadata has changed | [optional] 

## Example

```python
from polaris.management.models.catalog_role import CatalogRole

# TODO update the JSON string below
json = "{}"
# create an instance of CatalogRole from a JSON string
catalog_role_instance = CatalogRole.from_json(json)
# print the JSON string representation of the object
print(CatalogRole.to_json())

# convert the object into a dict
catalog_role_dict = catalog_role_instance.to_dict()
# create an instance of CatalogRole from a dict
catalog_role_from_dict = CatalogRole.from_dict(catalog_role_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


