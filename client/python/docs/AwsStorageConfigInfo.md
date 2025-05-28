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
# AwsStorageConfigInfo

aws storage configuration info

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**role_arn** | **str** | the aws role arn that grants privileges on the S3 buckets | 
**external_id** | **str** | an optional external id used to establish a trust relationship with AWS in the trust policy | [optional] 
**user_arn** | **str** | the aws user arn used to assume the aws role | [optional] 
**region** | **str** | the aws region where data is stored | [optional] 

## Example

```python
from polaris.management.models.aws_storage_config_info import AwsStorageConfigInfo

# TODO update the JSON string below
json = "{}"
# create an instance of AwsStorageConfigInfo from a JSON string
aws_storage_config_info_instance = AwsStorageConfigInfo.from_json(json)
# print the JSON string representation of the object
print(AwsStorageConfigInfo.to_json())

# convert the object into a dict
aws_storage_config_info_dict = aws_storage_config_info_instance.to_dict()
# create an instance of AwsStorageConfigInfo from a dict
aws_storage_config_info_from_dict = AwsStorageConfigInfo.from_dict(aws_storage_config_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


