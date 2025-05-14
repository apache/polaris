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
# polaris.catalog.GenericTableAPI

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_generic_table**](GenericTableAPI.md#create_generic_table) | **POST** /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables | Create a generic table under the given namespace
[**drop_generic_table**](GenericTableAPI.md#drop_generic_table) | **DELETE** /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table} | Drop a generic table under the given namespace from the catalog
[**list_generic_tables**](GenericTableAPI.md#list_generic_tables) | **GET** /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables | List all generic tables identifiers underneath a given namespace
[**load_generic_table**](GenericTableAPI.md#load_generic_table) | **GET** /polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table} | Load a generic table under the given namespace from the catalog


# **create_generic_table**
> LoadGenericTableResponse create_generic_table(prefix, namespace, create_generic_table_request)

Create a generic table under the given namespace

Create a generic table under the given namespace, and return the created table information as a response.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.create_generic_table_request import CreateGenericTableRequest
from polaris.catalog.models.load_generic_table_response import LoadGenericTableResponse
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
    api_instance = polaris.catalog.GenericTableAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    create_generic_table_request = polaris.catalog.CreateGenericTableRequest() # CreateGenericTableRequest | 

    try:
        # Create a generic table under the given namespace
        api_response = api_instance.create_generic_table(prefix, namespace, create_generic_table_request)
        print("The response of GenericTableAPI->create_generic_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GenericTableAPI->create_generic_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **create_generic_table_request** | [**CreateGenericTableRequest**](CreateGenericTableRequest.md)|  | 

### Return type

[**LoadGenericTableResponse**](LoadGenericTableResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Table result if successfully created a generic table. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**409** | Conflict - The table already exists under the given namespace |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **drop_generic_table**
> drop_generic_table(prefix, namespace, generic_table)

Drop a generic table under the given namespace from the catalog

Remove a table under the given namespace from the catalog

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
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
    api_instance = polaris.catalog.GenericTableAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    generic_table = 'sales' # str | A generic table name

    try:
        # Drop a generic table under the given namespace from the catalog
        api_instance.drop_generic_table(prefix, namespace, generic_table)
    except Exception as e:
        print("Exception when calling GenericTableAPI->drop_generic_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **generic_table** | **str**| A generic table name | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableError, Generic table to drop does not exist |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_generic_tables**
> ListGenericTablesResponse list_generic_tables(prefix, namespace, page_token=page_token, page_size=page_size)

List all generic tables identifiers underneath a given namespace

Return all generic table identifiers under this namespace

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.list_generic_tables_response import ListGenericTablesResponse
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
    api_instance = polaris.catalog.GenericTableAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    page_token = 'page_token_example' # str |  (optional)
    page_size = 56 # int | For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`. (optional)

    try:
        # List all generic tables identifiers underneath a given namespace
        api_response = api_instance.list_generic_tables(prefix, namespace, page_token=page_token, page_size=page_size)
        print("The response of GenericTableAPI->list_generic_tables:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GenericTableAPI->list_generic_tables: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **page_token** | **str**|  | [optional] 
 **page_size** | **int**| For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;. | [optional] 

### Return type

[**ListGenericTablesResponse**](ListGenericTablesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of generic table identifiers. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **load_generic_table**
> LoadGenericTableResponse load_generic_table(prefix, namespace, generic_table)

Load a generic table under the given namespace from the catalog

Load a generic table from the catalog under the given namespace.
The response contains all table information passed during create.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.load_generic_table_response import LoadGenericTableResponse
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
    api_instance = polaris.catalog.GenericTableAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    generic_table = 'sales' # str | A generic table name

    try:
        # Load a generic table under the given namespace from the catalog
        api_response = api_instance.load_generic_table(prefix, namespace, generic_table)
        print("The response of GenericTableAPI->load_generic_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GenericTableAPI->load_generic_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **generic_table** | **str**| A generic table name | 

### Return type

[**LoadGenericTableResponse**](LoadGenericTableResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Table result if successfully load a generic table. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableError, generic table to load does not exist |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

