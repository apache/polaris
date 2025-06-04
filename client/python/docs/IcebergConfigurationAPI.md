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
# polaris.catalog.IcebergConfigurationAPI

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_config**](IcebergConfigurationAPI.md#get_config) | **GET** /v1/config | List all catalog configuration settings


# **get_config**
> CatalogConfig get_config(warehouse=warehouse)

List all catalog configuration settings

 All REST clients should first call this route to get catalog configuration properties from the server to configure the catalog and its HTTP client. Configuration from the server consists of two sets of key/value pairs.
- defaults -  properties that should be used as default configuration; applied before client configuration
- overrides - properties that should be used to override client configuration; applied after defaults and client configuration

Catalog configuration is constructed by setting the defaults, then client- provided configuration, and finally overrides. The final property set is then used to configure the catalog.

For example, a default configuration property might set the size of the client pool, which can be replaced with a client-specific setting. An override might be used to set the warehouse location, which is stored on the server rather than in client configuration.

Common catalog configuration settings are documented at https://iceberg.apache.org/docs/latest/configuration/#catalog-properties

The catalog configuration also holds an optional `endpoints` field that contains information about the endpoints supported by the server. If a server does not send the `endpoints` field, a default set of endpoints is assumed:
- GET /v1/{prefix}/namespaces
- POST /v1/{prefix}/namespaces
- GET /v1/{prefix}/namespaces/{namespace}
- DELETE /v1/{prefix}/namespaces/{namespace}
- POST /v1/{prefix}/namespaces/{namespace}/properties
- GET /v1/{prefix}/namespaces/{namespace}/tables
- POST /v1/{prefix}/namespaces/{namespace}/tables
- GET /v1/{prefix}/namespaces/{namespace}/tables/{table}
- POST /v1/{prefix}/namespaces/{namespace}/tables/{table}
- DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}
- POST /v1/{prefix}/namespaces/{namespace}/register
- POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics
- POST /v1/{prefix}/tables/rename
- POST /v1/{prefix}/transactions/commit 

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.catalog_config import CatalogConfig
from polaris.catalog.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.catalog.Configuration(
    host = "https://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Configure Bearer authorization: BearerAuth
configuration = polaris.catalog.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
with polaris.catalog.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.catalog.IcebergConfigurationAPI(api_client)
    warehouse = 'warehouse_example' # str | Warehouse location or identifier to request from the service (optional)

    try:
        # List all catalog configuration settings
        api_response = api_instance.get_config(warehouse=warehouse)
        print("The response of IcebergConfigurationAPI->get_config:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling IcebergConfigurationAPI->get_config: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **warehouse** | **str**| Warehouse location or identifier to request from the service | [optional] 

### Return type

[**CatalogConfig**](CatalogConfig.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Server specified configuration values. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

