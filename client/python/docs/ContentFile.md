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
# ContentFile

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**content** | **str** |  | 
**file_path** | **str** |  | 
**file_format** | [**FileFormat**](FileFormat.md) |  | 
**spec_id** | **int** |  | 
**partition** | [**List[PrimitiveTypeValue]**](PrimitiveTypeValue.md) | A list of partition field values ordered based on the fields of the partition spec specified by the &#x60;spec-id&#x60; | [optional] 
**file_size_in_bytes** | **int** | Total file size in bytes | 
**record_count** | **int** | Number of records in the file | 
**key_metadata** | **str** | Encryption key metadata blob | [optional] 
**split_offsets** | **List[int]** | List of splittable offsets | [optional] 
**sort_order_id** | **int** |  | [optional] 

## Example

```python
from polaris.catalog.models.content_file import ContentFile

# TODO update the JSON string below
json = "{}"
# create an instance of ContentFile from a JSON string
content_file_instance = ContentFile.from_json(json)
# print the JSON string representation of the object
print(ContentFile.to_json())

# convert the object into a dict
content_file_dict = content_file_instance.to_dict()
# create an instance of ContentFile from a dict
content_file_from_dict = ContentFile.from_dict(content_file_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


