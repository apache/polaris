<!--

 Copyright (c) 2024 Snowflake Computing Inc.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->
# PolarisCatalog

The base catalog type - this contains all the fields necessary to construct an INTERNAL catalog

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------

## Example

```python
from polaris.management.models.polaris_catalog import PolarisCatalog

# TODO update the JSON string below
json = "{}"
# create an instance of PolarisCatalog from a JSON string
polaris_catalog_instance = PolarisCatalog.from_json(json)
# print the JSON string representation of the object
print(PolarisCatalog.to_json())

# convert the object into a dict
polaris_catalog_dict = polaris_catalog_instance.to_dict()
# create an instance of PolarisCatalog from a dict
polaris_catalog_from_dict = PolarisCatalog.from_dict(polaris_catalog_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


