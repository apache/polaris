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
# AzureStorageConfigInfo

azure storage configuration info

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tenant_id** | **str** | the tenant id that the storage accounts belong to | 
**multi_tenant_app_name** | **str** | the name of the azure client application | [optional] 
**consent_url** | **str** | URL to the Azure permissions request page | [optional] 

## Example

```python
from polaris.management.models.azure_storage_config_info import AzureStorageConfigInfo

# TODO update the JSON string below
json = "{}"
# create an instance of AzureStorageConfigInfo from a JSON string
azure_storage_config_info_instance = AzureStorageConfigInfo.from_json(json)
# print the JSON string representation of the object
print(AzureStorageConfigInfo.to_json())

# convert the object into a dict
azure_storage_config_info_dict = azure_storage_config_info_instance.to_dict()
# create an instance of AzureStorageConfigInfo from a dict
azure_storage_config_info_from_dict = AzureStorageConfigInfo.from_dict(azure_storage_config_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


