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
# StatisticsFile


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**snapshot_id** | **int** |  | 
**statistics_path** | **str** |  | 
**file_size_in_bytes** | **int** |  | 
**file_footer_size_in_bytes** | **int** |  | 
**blob_metadata** | [**List[BlobMetadata]**](BlobMetadata.md) |  | 

## Example

```python
from polaris.catalog.models.statistics_file import StatisticsFile

# TODO update the JSON string below
json = "{}"
# create an instance of StatisticsFile from a JSON string
statistics_file_instance = StatisticsFile.from_json(json)
# print the JSON string representation of the object
print(StatisticsFile.to_json())

# convert the object into a dict
statistics_file_dict = statistics_file_instance.to_dict()
# create an instance of StatisticsFile from a dict
statistics_file_from_dict = StatisticsFile.from_dict(statistics_file_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


