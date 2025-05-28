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
# GenericTable

Generic Table information. - `name` name for the generic table - `format` format for the generic table, i.e. \"delta\", \"csv\" - `properties` properties for the generic table passed on creation - `doc` comment or description for the generic table 

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**format** | **str** |  | 
**doc** | **str** |  | [optional] 
**properties** | **Dict[str, str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.generic_table import GenericTable

# TODO update the JSON string below
json = "{}"
# create an instance of GenericTable from a JSON string
generic_table_instance = GenericTable.from_json(json)
# print the JSON string representation of the object
print(GenericTable.to_json())

# convert the object into a dict
generic_table_dict = generic_table_instance.to_dict()
# create an instance of GenericTable from a dict
generic_table_from_dict = GenericTable.from_dict(generic_table_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


