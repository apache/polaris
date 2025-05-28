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
# LoadTableResult

Result used when a table is successfully loaded.   The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed. Clients can check whether metadata has changed by comparing metadata locations after the table has been created.   The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.   The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled   ## AWS Configurations  The following configurations should be respected when working with tables stored in AWS S3  - `client.region`: region to configure client for making requests to AWS  - `s3.access-key-id`: id for credentials that provide access to the data in S3  - `s3.secret-access-key`: secret for credentials that provide access to data in S3   - `s3.session-token`: if present, this value should be used for as the session token   - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the `s3-signer-open-api.yaml` specification  - `s3.cross-region-access-enabled`: if `true`, S3 Cross-Region bucket access is enabled  ## Storage Credentials  Credentials for ADLS / GCS / S3 / ... are provided through the `storage-credentials` field. Clients must first check whether the respective credentials exist in the `storage-credentials` field before checking the `config` for credentials. 

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**metadata_location** | **str** | May be null if the table is staged as part of a transaction | [optional] 
**metadata** | [**TableMetadata**](TableMetadata.md) |  | 
**config** | **Dict[str, str]** |  | [optional] 
**storage_credentials** | [**List[StorageCredential]**](StorageCredential.md) |  | [optional] 

## Example

```python
from polaris.catalog.models.load_table_result import LoadTableResult

# TODO update the JSON string below
json = "{}"
# create an instance of LoadTableResult from a JSON string
load_table_result_instance = LoadTableResult.from_json(json)
# print the JSON string representation of the object
print(LoadTableResult.to_json())

# convert the object into a dict
load_table_result_dict = load_table_result_instance.to_dict()
# create an instance of LoadTableResult from a dict
load_table_result_from_dict = LoadTableResult.from_dict(load_table_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


