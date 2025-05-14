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
# IcebergRestConnectionConfigInfo

Configuration necessary for connecting to an Iceberg REST Catalog

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**remote_catalog_name** | **str** | The name of a remote catalog instance within the remote catalog service; in some older systems this is specified as the &#39;warehouse&#39; when multiple logical catalogs are served under the same base uri, and often translates into a &#39;prefix&#39; added to all REST resource paths | [optional] 

## Example

```python
from polaris.management.models.iceberg_rest_connection_config_info import IcebergRestConnectionConfigInfo

# TODO update the JSON string below
json = "{}"
# create an instance of IcebergRestConnectionConfigInfo from a JSON string
iceberg_rest_connection_config_info_instance = IcebergRestConnectionConfigInfo.from_json(json)
# print the JSON string representation of the object
print(IcebergRestConnectionConfigInfo.to_json())

# convert the object into a dict
iceberg_rest_connection_config_info_dict = iceberg_rest_connection_config_info_instance.to_dict()
# create an instance of IcebergRestConnectionConfigInfo from a dict
iceberg_rest_connection_config_info_from_dict = IcebergRestConnectionConfigInfo.from_dict(iceberg_rest_connection_config_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


