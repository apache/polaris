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
# StorageCredential


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**prefix** | **str** | Indicates a storage location prefix where the credential is relevant. Clients should choose the most specific prefix (by selecting the longest prefix) if several credentials of the same type are available. | 
**config** | **Dict[str, str]** |  | 

## Example

```python
from polaris.catalog.models.storage_credential import StorageCredential

# TODO update the JSON string below
json = "{}"
# create an instance of StorageCredential from a JSON string
storage_credential_instance = StorageCredential.from_json(json)
# print the JSON string representation of the object
print(StorageCredential.to_json())

# convert the object into a dict
storage_credential_dict = storage_credential_instance.to_dict()
# create an instance of StorageCredential from a dict
storage_credential_from_dict = StorageCredential.from_dict(storage_credential_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


