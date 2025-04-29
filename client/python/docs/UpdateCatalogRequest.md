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
# UpdateCatalogRequest

Updates to apply to a Catalog. Any fields which are required in the Catalog will remain unaltered if omitted from the contents of this Update request.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**current_entity_version** | **int** | The version of the object onto which this update is applied; if the object changed, the update will fail and the caller should retry after fetching the latest version. | [optional] 
**properties** | **Dict[str, str]** |  | [optional] 
**storage_config_info** | [**StorageConfigInfo**](StorageConfigInfo.md) |  | [optional] 

## Example

```python
from polaris.management.models.update_catalog_request import UpdateCatalogRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateCatalogRequest from a JSON string
update_catalog_request_instance = UpdateCatalogRequest.from_json(json)
# print the JSON string representation of the object
print(UpdateCatalogRequest.to_json())

# convert the object into a dict
update_catalog_request_dict = update_catalog_request_instance.to_dict()
# create an instance of UpdateCatalogRequest from a dict
update_catalog_request_from_dict = UpdateCatalogRequest.from_dict(update_catalog_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


