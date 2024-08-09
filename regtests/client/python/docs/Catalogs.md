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
# Catalogs

A list of Catalog objects

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**catalogs** | [**List[Catalog]**](Catalog.md) |  | 

## Example

```python
from polaris.management.models.catalogs import Catalogs

# TODO update the JSON string below
json = "{}"
# create an instance of Catalogs from a JSON string
catalogs_instance = Catalogs.from_json(json)
# print the JSON string representation of the object
print(Catalogs.to_json())

# convert the object into a dict
catalogs_dict = catalogs_instance.to_dict()
# create an instance of Catalogs from a dict
catalogs_from_dict = Catalogs.from_dict(catalogs_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


