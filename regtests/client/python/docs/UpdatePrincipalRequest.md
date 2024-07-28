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
# UpdatePrincipalRequest

Updates to apply to a Principal

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**current_entity_version** | **int** | The version of the object onto which this update is applied; if the object changed, the update will fail and the caller should retry after fetching the latest version. | 
**properties** | **Dict[str, str]** |  | 

## Example

```python
from polaris.management.models.update_principal_request import UpdatePrincipalRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdatePrincipalRequest from a JSON string
update_principal_request_instance = UpdatePrincipalRequest.from_json(json)
# print the JSON string representation of the object
print(UpdatePrincipalRequest.to_json())

# convert the object into a dict
update_principal_request_dict = update_principal_request_instance.to_dict()
# create an instance of UpdatePrincipalRequest from a dict
update_principal_request_from_dict = UpdatePrincipalRequest.from_dict(update_principal_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


