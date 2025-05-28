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
# Catalog

A catalog object. A catalog may be internal or external. External catalogs are managed entirely by an external catalog interface. Third party catalogs may be other Iceberg REST implementations or other services with their own proprietary APIs

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** | the type of catalog - internal or external | [default to 'INTERNAL']
**name** | **str** | The name of the catalog | 
**properties** | [**CatalogProperties**](CatalogProperties.md) |  | 
**create_timestamp** | **int** | The creation time represented as unix epoch timestamp in milliseconds | [optional] 
**last_update_timestamp** | **int** | The last update time represented as unix epoch timestamp in milliseconds | [optional] 
**entity_version** | **int** | The version of the catalog object used to determine if the catalog metadata has changed | [optional] 
**storage_config_info** | [**StorageConfigInfo**](StorageConfigInfo.md) |  | 

## Example

```python
from polaris.management.models.catalog import Catalog

# TODO update the JSON string below
json = "{}"
# create an instance of Catalog from a JSON string
catalog_instance = Catalog.from_json(json)
# print the JSON string representation of the object
print(Catalog.to_json())

# convert the object into a dict
catalog_dict = catalog_instance.to_dict()
# create an instance of Catalog from a dict
catalog_from_dict = Catalog.from_dict(catalog_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


