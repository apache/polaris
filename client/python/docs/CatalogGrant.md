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
# CatalogGrant


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**privilege** | [**CatalogPrivilege**](CatalogPrivilege.md) |  | 

## Example

```python
from polaris.management.models.catalog_grant import CatalogGrant

# TODO update the JSON string below
json = "{}"
# create an instance of CatalogGrant from a JSON string
catalog_grant_instance = CatalogGrant.from_json(json)
# print the JSON string representation of the object
print(CatalogGrant.to_json())

# convert the object into a dict
catalog_grant_dict = catalog_grant_instance.to_dict()
# create an instance of CatalogGrant from a dict
catalog_grant_from_dict = CatalogGrant.from_dict(catalog_grant_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


