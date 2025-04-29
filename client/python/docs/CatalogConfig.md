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
# CatalogConfig

Server-provided configuration for the catalog.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**overrides** | **Dict[str, str]** | Properties that should be used to override client configuration; applied after defaults and client configuration. | 
**defaults** | **Dict[str, str]** | Properties that should be used as default configuration; applied before client configuration. | 
**endpoints** | **List[str]** | A list of endpoints that the server supports. The format of each endpoint must be \&quot;&lt;HTTP verb&gt; &lt;resource path from OpenAPI REST spec&gt;\&quot;. The HTTP verb and the resource path must be separated by a space character. | [optional] 

## Example

```python
from polaris.catalog.models.catalog_config import CatalogConfig

# TODO update the JSON string below
json = "{}"
# create an instance of CatalogConfig from a JSON string
catalog_config_instance = CatalogConfig.from_json(json)
# print the JSON string representation of the object
print(CatalogConfig.to_json())

# convert the object into a dict
catalog_config_dict = catalog_config_instance.to_dict()
# create an instance of CatalogConfig from a dict
catalog_config_from_dict = CatalogConfig.from_dict(catalog_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


