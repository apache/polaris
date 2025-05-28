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
# SortOrder


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**order_id** | **int** |  | [readonly] 
**fields** | [**List[SortField]**](SortField.md) |  | 

## Example

```python
from polaris.catalog.models.sort_order import SortOrder

# TODO update the JSON string below
json = "{}"
# create an instance of SortOrder from a JSON string
sort_order_instance = SortOrder.from_json(json)
# print the JSON string representation of the object
print(SortOrder.to_json())

# convert the object into a dict
sort_order_dict = sort_order_instance.to_dict()
# create an instance of SortOrder from a dict
sort_order_from_dict = SortOrder.from_dict(sort_order_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


