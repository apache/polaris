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
# polaris.catalog.CatalogAPI

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**commit_transaction**](CatalogAPI.md#commit_transaction) | **POST** /v1/{prefix}/transactions/commit | Commit updates to multiple tables in an atomic operation
[**create_namespace**](CatalogAPI.md#create_namespace) | **POST** /v1/{prefix}/namespaces | Create a namespace
[**create_table**](CatalogAPI.md#create_table) | **POST** /v1/{prefix}/namespaces/{namespace}/tables | Create a table in the given namespace
[**create_view**](CatalogAPI.md#create_view) | **POST** /v1/{prefix}/namespaces/{namespace}/views | Create a view in the given namespace
[**drop_namespace**](CatalogAPI.md#drop_namespace) | **DELETE** /v1/{prefix}/namespaces/{namespace} | Drop a namespace from the catalog. Namespace must be empty.
[**drop_table**](CatalogAPI.md#drop_table) | **DELETE** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Drop a table from the catalog
[**drop_view**](CatalogAPI.md#drop_view) | **DELETE** /v1/{prefix}/namespaces/{namespace}/views/{view} | Drop a view from the catalog
[**list_namespaces**](CatalogAPI.md#list_namespaces) | **GET** /v1/{prefix}/namespaces | List namespaces, optionally providing a parent namespace to list underneath
[**list_tables**](CatalogAPI.md#list_tables) | **GET** /v1/{prefix}/namespaces/{namespace}/tables | List all table identifiers underneath a given namespace
[**list_views**](CatalogAPI.md#list_views) | **GET** /v1/{prefix}/namespaces/{namespace}/views | List all view identifiers underneath a given namespace
[**load_credentials**](CatalogAPI.md#load_credentials) | **GET** /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials | Load vended credentials for a table from the catalog
[**load_namespace_metadata**](CatalogAPI.md#load_namespace_metadata) | **GET** /v1/{prefix}/namespaces/{namespace} | Load the metadata properties for a namespace
[**load_table**](CatalogAPI.md#load_table) | **GET** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Load a table from the catalog
[**load_view**](CatalogAPI.md#load_view) | **GET** /v1/{prefix}/namespaces/{namespace}/views/{view} | Load a view from the catalog
[**namespace_exists**](CatalogAPI.md#namespace_exists) | **HEAD** /v1/{prefix}/namespaces/{namespace} | Check if a namespace exists
[**register_table**](CatalogAPI.md#register_table) | **POST** /v1/{prefix}/namespaces/{namespace}/register | Register a table in the given namespace using given metadata file location
[**rename_table**](CatalogAPI.md#rename_table) | **POST** /v1/{prefix}/tables/rename | Rename a table from its current name to a new name
[**rename_view**](CatalogAPI.md#rename_view) | **POST** /v1/{prefix}/views/rename | Rename a view from its current name to a new name
[**replace_view**](CatalogAPI.md#replace_view) | **POST** /v1/{prefix}/namespaces/{namespace}/views/{view} | Replace a view
[**report_metrics**](CatalogAPI.md#report_metrics) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics | Send a metrics report to this endpoint to be processed by the backend
[**send_notification**](CatalogAPI.md#send_notification) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table}/notifications | Sends a notification to the table
[**table_exists**](CatalogAPI.md#table_exists) | **HEAD** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Check if a table exists
[**update_properties**](CatalogAPI.md#update_properties) | **POST** /v1/{prefix}/namespaces/{namespace}/properties | Set or remove properties on a namespace
[**update_table**](CatalogAPI.md#update_table) | **POST** /v1/{prefix}/namespaces/{namespace}/tables/{table} | Commit updates to a table
[**view_exists**](CatalogAPI.md#view_exists) | **HEAD** /v1/{prefix}/namespaces/{namespace}/views/{view} | Check if a view exists


# **commit_transaction**
> commit_transaction(prefix, commit_transaction_request)

Commit updates to multiple tables in an atomic operation

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.commit_transaction_request import CommitTransactionRequest
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    commit_transaction_request = polaris.catalog.CommitTransactionRequest() # CommitTransactionRequest | Commit updates to multiple tables in an atomic operation  A commit for a single table consists of a table identifier with requirements and updates. Requirements are assertions that will be validated before attempting to make and commit changes. For example, `assert-ref-snapshot-id` will check that a named ref's snapshot ID has a certain value. Server implementations are required to fail with a 400 status code if any unknown updates or requirements are received. Updates are changes to make to table metadata. For example, after asserting that the current main ref is at the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id.

    try:
        # Commit updates to multiple tables in an atomic operation
        api_instance.commit_transaction(prefix, commit_transaction_request)
    except Exception as e:
        print("Exception when calling CatalogAPI->commit_transaction: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **commit_transaction_request** | [**CommitTransactionRequest**](CommitTransactionRequest.md)| Commit updates to multiple tables in an atomic operation  A commit for a single table consists of a table identifier with requirements and updates. Requirements are assertions that will be validated before attempting to make and commit changes. For example, &#x60;assert-ref-snapshot-id&#x60; will check that a named ref&#39;s snapshot ID has a certain value. Server implementations are required to fail with a 400 status code if any unknown updates or requirements are received. Updates are changes to make to table metadata. For example, after asserting that the current main ref is at the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id. | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableException, table to load does not exist |  -  |
**409** | Conflict - CommitFailedException, one or more requirements failed. The client may retry. |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**500** | An unknown server-side problem occurred; the commit state is unknown. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**502** | A gateway or proxy received an invalid response from the upstream server; the commit state is unknown. |  -  |
**504** | A server-side gateway timeout occurred; the commit state is unknown. |  -  |
**5XX** | A server-side problem that might not be addressable on the client. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_namespace**
> CreateNamespaceResponse create_namespace(prefix, create_namespace_request)

Create a namespace

Create a namespace, with an optional set of properties. The server might also add properties, such as `last_modified_time` etc.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.create_namespace_request import CreateNamespaceRequest
from polaris.catalog.models.create_namespace_response import CreateNamespaceResponse
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    create_namespace_request = polaris.catalog.CreateNamespaceRequest() # CreateNamespaceRequest | 

    try:
        # Create a namespace
        api_response = api_instance.create_namespace(prefix, create_namespace_request)
        print("The response of CatalogAPI->create_namespace:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->create_namespace: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **create_namespace_request** | [**CreateNamespaceRequest**](CreateNamespaceRequest.md)|  | 

### Return type

[**CreateNamespaceResponse**](CreateNamespaceResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Represents a successful call to create a namespace. Returns the namespace created, as well as any properties that were stored for the namespace, including those the server might have added. Implementations are not required to support namespace properties. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**406** | Not Acceptable / Unsupported Operation. The server does not support this operation. |  -  |
**409** | Conflict - The namespace already exists |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_table**
> LoadTableResult create_table(prefix, namespace, create_table_request, x_iceberg_access_delegation=x_iceberg_access_delegation)

Create a table in the given namespace

Create a table or start a create transaction, like atomic CTAS.

If `stage-create` is false, the table is created immediately.

If `stage-create` is true, the table is not created, but table metadata is initialized and returned. The service should prepare as needed for a commit to the table commit endpoint to complete the create transaction. The client uses the returned metadata to begin a transaction. To commit the transaction, the client sends all create and subsequent changes to the table commit route. Changes from the table create operation include changes like AddSchemaUpdate and SetCurrentSchemaUpdate that set the initial table state.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.create_table_request import CreateTableRequest
from polaris.catalog.models.load_table_result import LoadTableResult
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    create_table_request = polaris.catalog.CreateTableRequest() # CreateTableRequest | 
    x_iceberg_access_delegation = 'vended-credentials,remote-signing' # str | Optional signal to the server that the client supports delegated access via a comma-separated list of access mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.  Specific properties and handling for `vended-credentials` is documented in the `LoadTableResult` schema section of this spec document.  The protocol and specification for `remote-signing` is documented in  the `s3-signer-open-api.yaml` OpenApi spec in the `aws` module.  (optional)

    try:
        # Create a table in the given namespace
        api_response = api_instance.create_table(prefix, namespace, create_table_request, x_iceberg_access_delegation=x_iceberg_access_delegation)
        print("The response of CatalogAPI->create_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->create_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **create_table_request** | [**CreateTableRequest**](CreateTableRequest.md)|  | 
 **x_iceberg_access_delegation** | **str**| Optional signal to the server that the client supports delegated access via a comma-separated list of access mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.  Specific properties and handling for &#x60;vended-credentials&#x60; is documented in the &#x60;LoadTableResult&#x60; schema section of this spec document.  The protocol and specification for &#x60;remote-signing&#x60; is documented in  the &#x60;s3-signer-open-api.yaml&#x60; OpenApi spec in the &#x60;aws&#x60; module.  | [optional] 

### Return type

[**LoadTableResult**](LoadTableResult.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Table metadata result after creating a table |  * etag -  <br>  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**409** | Conflict - The table already exists |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_view**
> LoadViewResult create_view(prefix, namespace, create_view_request)

Create a view in the given namespace

Create a view in the given namespace.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.create_view_request import CreateViewRequest
from polaris.catalog.models.load_view_result import LoadViewResult
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    create_view_request = polaris.catalog.CreateViewRequest() # CreateViewRequest | 

    try:
        # Create a view in the given namespace
        api_response = api_instance.create_view(prefix, namespace, create_view_request)
        print("The response of CatalogAPI->create_view:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->create_view: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **create_view_request** | [**CreateViewRequest**](CreateViewRequest.md)|  | 

### Return type

[**LoadViewResult**](LoadViewResult.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | View metadata result when loading a view |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**409** | Conflict - The view already exists |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **drop_namespace**
> drop_namespace(prefix, namespace)

Drop a namespace from the catalog. Namespace must be empty.

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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.

    try:
        # Drop a namespace from the catalog. Namespace must be empty.
        api_instance.drop_namespace(prefix, namespace)
    except Exception as e:
        print("Exception when calling CatalogAPI->drop_namespace: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 

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
**404** | Not Found - Namespace to delete does not exist. |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **drop_table**
> drop_table(prefix, namespace, table, purge_requested=purge_requested)

Drop a table from the catalog

Remove a table from the catalog

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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    table = 'sales' # str | A table name
    purge_requested = False # bool | Whether the user requested to purge the underlying table's data and metadata (optional) (default to False)

    try:
        # Drop a table from the catalog
        api_instance.drop_table(prefix, namespace, table, purge_requested=purge_requested)
    except Exception as e:
        print("Exception when calling CatalogAPI->drop_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **table** | **str**| A table name | 
 **purge_requested** | **bool**| Whether the user requested to purge the underlying table&#39;s data and metadata | [optional] [default to False]

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
**404** | Not Found - NoSuchTableException, Table to drop does not exist |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **drop_view**
> drop_view(prefix, namespace, view)

Drop a view from the catalog

Remove a view from the catalog

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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    view = 'sales' # str | A view name

    try:
        # Drop a view from the catalog
        api_instance.drop_view(prefix, namespace, view)
    except Exception as e:
        print("Exception when calling CatalogAPI->drop_view: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **view** | **str**| A view name | 

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
**404** | Not Found - NoSuchViewException, view to drop does not exist |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_namespaces**
> ListNamespacesResponse list_namespaces(prefix, page_token=page_token, page_size=page_size, parent=parent)

List namespaces, optionally providing a parent namespace to list underneath

List all namespaces at a certain level, optionally starting from a given parent namespace. If table accounting.tax.paid.info exists, using 'SELECT NAMESPACE IN accounting' would translate into `GET /namespaces?parent=accounting` and must return a namespace, ["accounting", "tax"] only. Using 'SELECT NAMESPACE IN accounting.tax' would translate into `GET /namespaces?parent=accounting%1Ftax` and must return a namespace, ["accounting", "tax", "paid"]. If `parent` is not provided, all top-level namespaces should be listed.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.list_namespaces_response import ListNamespacesResponse
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    page_token = 'page_token_example' # str |  (optional)
    page_size = 56 # int | For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`. (optional)
    parent = 'accounting%1Ftax' # str | An optional namespace, underneath which to list namespaces. If not provided or empty, all top-level namespaces should be listed. If parent is a multipart namespace, the parts must be separated by the unit separator (`0x1F`) byte. (optional)

    try:
        # List namespaces, optionally providing a parent namespace to list underneath
        api_response = api_instance.list_namespaces(prefix, page_token=page_token, page_size=page_size, parent=parent)
        print("The response of CatalogAPI->list_namespaces:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->list_namespaces: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **page_token** | **str**|  | [optional] 
 **page_size** | **int**| For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;. | [optional] 
 **parent** | **str**| An optional namespace, underneath which to list namespaces. If not provided or empty, all top-level namespaces should be listed. If parent is a multipart namespace, the parts must be separated by the unit separator (&#x60;0x1F&#x60;) byte. | [optional] 

### Return type

[**ListNamespacesResponse**](ListNamespacesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A list of namespaces |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - Namespace provided in the &#x60;parent&#x60; query parameter is not found. |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_tables**
> ListTablesResponse list_tables(prefix, namespace, page_token=page_token, page_size=page_size)

List all table identifiers underneath a given namespace

Return all table identifiers under this namespace

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.list_tables_response import ListTablesResponse
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    page_token = 'page_token_example' # str |  (optional)
    page_size = 56 # int | For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`. (optional)

    try:
        # List all table identifiers underneath a given namespace
        api_response = api_instance.list_tables(prefix, namespace, page_token=page_token, page_size=page_size)
        print("The response of CatalogAPI->list_tables:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->list_tables: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **page_token** | **str**|  | [optional] 
 **page_size** | **int**| For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;. | [optional] 

### Return type

[**ListTablesResponse**](ListTablesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A list of table identifiers |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_views**
> ListTablesResponse list_views(prefix, namespace, page_token=page_token, page_size=page_size)

List all view identifiers underneath a given namespace

Return all view identifiers under this namespace

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.list_tables_response import ListTablesResponse
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    page_token = 'page_token_example' # str |  (optional)
    page_size = 56 # int | For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`. (optional)

    try:
        # List all view identifiers underneath a given namespace
        api_response = api_instance.list_views(prefix, namespace, page_token=page_token, page_size=page_size)
        print("The response of CatalogAPI->list_views:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->list_views: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **page_token** | **str**|  | [optional] 
 **page_size** | **int**| For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;. | [optional] 

### Return type

[**ListTablesResponse**](ListTablesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A list of table identifiers |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **load_credentials**
> LoadCredentialsResponse load_credentials(prefix, namespace, table)

Load vended credentials for a table from the catalog

Load vended credentials for a table from the catalog.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.load_credentials_response import LoadCredentialsResponse
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    table = 'sales' # str | A table name

    try:
        # Load vended credentials for a table from the catalog
        api_response = api_instance.load_credentials(prefix, namespace, table)
        print("The response of CatalogAPI->load_credentials:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->load_credentials: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **table** | **str**| A table name | 

### Return type

[**LoadCredentialsResponse**](LoadCredentialsResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Table credentials result when loading credentials for a table |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableException, table to load credentials for does not exist |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **load_namespace_metadata**
> GetNamespaceResponse load_namespace_metadata(prefix, namespace)

Load the metadata properties for a namespace

Return all stored metadata properties for a given namespace

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.get_namespace_response import GetNamespaceResponse
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.

    try:
        # Load the metadata properties for a namespace
        api_response = api_instance.load_namespace_metadata(prefix, namespace)
        print("The response of CatalogAPI->load_namespace_metadata:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->load_namespace_metadata: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 

### Return type

[**GetNamespaceResponse**](GetNamespaceResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Returns a namespace, as well as any properties stored on the namespace if namespace properties are supported by the server. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - Namespace not found |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **load_table**
> LoadTableResult load_table(prefix, namespace, table, x_iceberg_access_delegation=x_iceberg_access_delegation, if_none_match=if_none_match, snapshots=snapshots)

Load a table from the catalog

Load a table from the catalog.

The response contains both configuration and table metadata. The configuration, if non-empty is used as additional configuration for the table that overrides catalog configuration. For example, this configuration may change the FileIO implementation to be used for the table.

The response also contains the table's full metadata, matching the table metadata JSON file.

The catalog configuration may contain credentials that should be used for subsequent requests for the table. The configuration key "token" is used to pass an access token to be used as a bearer token for table requests. Otherwise, a token may be passed using a RFC 8693 token type as a configuration key. For example, "urn:ietf:params:oauth:token-type:jwt=<JWT-token>".

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.load_table_result import LoadTableResult
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    table = 'sales' # str | A table name
    x_iceberg_access_delegation = 'vended-credentials,remote-signing' # str | Optional signal to the server that the client supports delegated access via a comma-separated list of access mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.  Specific properties and handling for `vended-credentials` is documented in the `LoadTableResult` schema section of this spec document.  The protocol and specification for `remote-signing` is documented in  the `s3-signer-open-api.yaml` OpenApi spec in the `aws` module.  (optional)
    if_none_match = 'if_none_match_example' # str | An optional header that allows the server to return 304 (Not Modified) if the metadata is current. The content is the value of the ETag received in a CreateTableResponse or LoadTableResponse. (optional)
    snapshots = 'snapshots_example' # str | The snapshots to return in the body of the metadata. Setting the value to `all` would return the full set of snapshots currently valid for the table. Setting the value to `refs` would load all snapshots referenced by branches or tags. Default if no param is provided is `all`. (optional)

    try:
        # Load a table from the catalog
        api_response = api_instance.load_table(prefix, namespace, table, x_iceberg_access_delegation=x_iceberg_access_delegation, if_none_match=if_none_match, snapshots=snapshots)
        print("The response of CatalogAPI->load_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->load_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **table** | **str**| A table name | 
 **x_iceberg_access_delegation** | **str**| Optional signal to the server that the client supports delegated access via a comma-separated list of access mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.  Specific properties and handling for &#x60;vended-credentials&#x60; is documented in the &#x60;LoadTableResult&#x60; schema section of this spec document.  The protocol and specification for &#x60;remote-signing&#x60; is documented in  the &#x60;s3-signer-open-api.yaml&#x60; OpenApi spec in the &#x60;aws&#x60; module.  | [optional] 
 **if_none_match** | **str**| An optional header that allows the server to return 304 (Not Modified) if the metadata is current. The content is the value of the ETag received in a CreateTableResponse or LoadTableResponse. | [optional] 
 **snapshots** | **str**| The snapshots to return in the body of the metadata. Setting the value to &#x60;all&#x60; would return the full set of snapshots currently valid for the table. Setting the value to &#x60;refs&#x60; would load all snapshots referenced by branches or tags. Default if no param is provided is &#x60;all&#x60;. | [optional] 

### Return type

[**LoadTableResult**](LoadTableResult.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Table metadata result when loading a table |  * etag -  <br>  |
**304** | Not Modified - Based on the content of the &#39;If-None-Match&#39; header the table metadata has not changed since. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableException, table to load does not exist |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **load_view**
> LoadViewResult load_view(prefix, namespace, view)

Load a view from the catalog

Load a view from the catalog.

The response contains both configuration and view metadata. The configuration, if non-empty is used as additional configuration for the view that overrides catalog configuration.

The response also contains the view's full metadata, matching the view metadata JSON file.

The catalog configuration may contain credentials that should be used for subsequent requests for the view. The configuration key "token" is used to pass an access token to be used as a bearer token for view requests. Otherwise, a token may be passed using a RFC 8693 token type as a configuration key. For example, "urn:ietf:params:oauth:token-type:jwt=<JWT-token>".

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.load_view_result import LoadViewResult
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    view = 'sales' # str | A view name

    try:
        # Load a view from the catalog
        api_response = api_instance.load_view(prefix, namespace, view)
        print("The response of CatalogAPI->load_view:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->load_view: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **view** | **str**| A view name | 

### Return type

[**LoadViewResult**](LoadViewResult.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | View metadata result when loading a view |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchViewException, view to load does not exist |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **namespace_exists**
> namespace_exists(prefix, namespace)

Check if a namespace exists

Check if a namespace exists. The response does not contain a body.

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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.

    try:
        # Check if a namespace exists
        api_instance.namespace_exists(prefix, namespace)
    except Exception as e:
        print("Exception when calling CatalogAPI->namespace_exists: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 

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
**404** | Not Found - Namespace not found |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **register_table**
> LoadTableResult register_table(prefix, namespace, register_table_request)

Register a table in the given namespace using given metadata file location

Register a table using given metadata file location.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.load_table_result import LoadTableResult
from polaris.catalog.models.register_table_request import RegisterTableRequest
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    register_table_request = polaris.catalog.RegisterTableRequest() # RegisterTableRequest | 

    try:
        # Register a table in the given namespace using given metadata file location
        api_response = api_instance.register_table(prefix, namespace, register_table_request)
        print("The response of CatalogAPI->register_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->register_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **register_table_request** | [**RegisterTableRequest**](RegisterTableRequest.md)|  | 

### Return type

[**LoadTableResult**](LoadTableResult.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Table metadata result when loading a table |  * etag -  <br>  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**409** | Conflict - The table already exists |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **rename_table**
> rename_table(prefix, rename_table_request)

Rename a table from its current name to a new name

Rename a table from one identifier to another. It's valid to move a table across namespaces, but the server implementation is not required to support it.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.rename_table_request import RenameTableRequest
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    rename_table_request = polaris.catalog.RenameTableRequest() # RenameTableRequest | Current table identifier to rename and new table identifier to rename to

    try:
        # Rename a table from its current name to a new name
        api_instance.rename_table(prefix, rename_table_request)
    except Exception as e:
        print("Exception when calling CatalogAPI->rename_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **rename_table_request** | [**RenameTableRequest**](RenameTableRequest.md)| Current table identifier to rename and new table identifier to rename to | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableException, Table to rename does not exist - NoSuchNamespaceException, The target namespace of the new table identifier does not exist |  -  |
**406** | Not Acceptable / Unsupported Operation. The server does not support this operation. |  -  |
**409** | Conflict - The target identifier to rename to already exists as a table or view |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **rename_view**
> rename_view(prefix, rename_table_request)

Rename a view from its current name to a new name

Rename a view from one identifier to another. It's valid to move a view across namespaces, but the server implementation is not required to support it.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.rename_table_request import RenameTableRequest
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    rename_table_request = polaris.catalog.RenameTableRequest() # RenameTableRequest | Current view identifier to rename and new view identifier to rename to

    try:
        # Rename a view from its current name to a new name
        api_instance.rename_view(prefix, rename_table_request)
    except Exception as e:
        print("Exception when calling CatalogAPI->rename_view: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **rename_table_request** | [**RenameTableRequest**](RenameTableRequest.md)| Current view identifier to rename and new view identifier to rename to | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchViewException, view to rename does not exist - NoSuchNamespaceException, The target namespace of the new identifier does not exist |  -  |
**406** | Not Acceptable / Unsupported Operation. The server does not support this operation. |  -  |
**409** | Conflict - The target identifier to rename to already exists as a table or view |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **replace_view**
> LoadViewResult replace_view(prefix, namespace, view, commit_view_request)

Replace a view

Commit updates to a view.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.commit_view_request import CommitViewRequest
from polaris.catalog.models.load_view_result import LoadViewResult
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    view = 'sales' # str | A view name
    commit_view_request = polaris.catalog.CommitViewRequest() # CommitViewRequest | 

    try:
        # Replace a view
        api_response = api_instance.replace_view(prefix, namespace, view, commit_view_request)
        print("The response of CatalogAPI->replace_view:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->replace_view: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **view** | **str**| A view name | 
 **commit_view_request** | [**CommitViewRequest**](CommitViewRequest.md)|  | 

### Return type

[**LoadViewResult**](LoadViewResult.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | View metadata result when loading a view |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchViewException, view to load does not exist |  -  |
**409** | Conflict - CommitFailedException. The client may retry. |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**500** | An unknown server-side problem occurred; the commit state is unknown. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**502** | A gateway or proxy received an invalid response from the upstream server; the commit state is unknown. |  -  |
**504** | A server-side gateway timeout occurred; the commit state is unknown. |  -  |
**5XX** | A server-side problem that might not be addressable on the client. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **report_metrics**
> report_metrics(prefix, namespace, table, report_metrics_request)

Send a metrics report to this endpoint to be processed by the backend

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.report_metrics_request import ReportMetricsRequest
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    table = 'sales' # str | A table name
    report_metrics_request = polaris.catalog.ReportMetricsRequest() # ReportMetricsRequest | The request containing the metrics report to be sent

    try:
        # Send a metrics report to this endpoint to be processed by the backend
        api_instance.report_metrics(prefix, namespace, table, report_metrics_request)
    except Exception as e:
        print("Exception when calling CatalogAPI->report_metrics: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **table** | **str**| A table name | 
 **report_metrics_request** | [**ReportMetricsRequest**](ReportMetricsRequest.md)| The request containing the metrics report to be sent | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableException, table to load does not exist |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **send_notification**
> send_notification(prefix, namespace, table, notification_request)

Sends a notification to the table

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.notification_request import NotificationRequest
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    table = 'sales' # str | A table name
    notification_request = polaris.catalog.NotificationRequest() # NotificationRequest | The request containing the notification to be sent. For each table, Polaris will reject any notification where the timestamp in the request body is older than or equal to the most recent time Polaris has already processed for the table. The responsibility of ensuring the correct order of timestamps for a sequence of notifications lies with the caller of the API. This includes managing potential clock skew or inconsistencies when notifications are sent from multiple sources. A VALIDATE request behaves like a dry-run of a CREATE or UPDATE request up to but not including loading the contents of a metadata file; this includes validations of permissions, the specified metadata path being within ALLOWED_LOCATIONS, having an EXTERNAL catalog, etc. The intended use case for a VALIDATE notification is to allow a remote catalog to pre-validate the general settings of a receiving catalog against an intended new table location before possibly creating a table intended for sending notifications in the remote catalog at all. For a VALIDATE request, the specified metadata-location can either be a prospective full metadata file path, or a relevant parent directory of the intended table to validate against ALLOWED_LOCATIONS.

    try:
        # Sends a notification to the table
        api_instance.send_notification(prefix, namespace, table, notification_request)
    except Exception as e:
        print("Exception when calling CatalogAPI->send_notification: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **table** | **str**| A table name | 
 **notification_request** | [**NotificationRequest**](NotificationRequest.md)| The request containing the notification to be sent. For each table, Polaris will reject any notification where the timestamp in the request body is older than or equal to the most recent time Polaris has already processed for the table. The responsibility of ensuring the correct order of timestamps for a sequence of notifications lies with the caller of the API. This includes managing potential clock skew or inconsistencies when notifications are sent from multiple sources. A VALIDATE request behaves like a dry-run of a CREATE or UPDATE request up to but not including loading the contents of a metadata file; this includes validations of permissions, the specified metadata path being within ALLOWED_LOCATIONS, having an EXTERNAL catalog, etc. The intended use case for a VALIDATE notification is to allow a remote catalog to pre-validate the general settings of a receiving catalog against an intended new table location before possibly creating a table intended for sending notifications in the remote catalog at all. For a VALIDATE request, the specified metadata-location can either be a prospective full metadata file path, or a relevant parent directory of the intended table to validate against ALLOWED_LOCATIONS. | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableException, table to load does not exist |  -  |
**409** | Conflict - The timestamp of the received notification is older than or equal to the most recent timestamp Polaris has already processed for the specified table. |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **table_exists**
> table_exists(prefix, namespace, table)

Check if a table exists

Check if a table exists within a given namespace. The response does not contain a body.

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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    table = 'sales' # str | A table name

    try:
        # Check if a table exists
        api_instance.table_exists(prefix, namespace, table)
    except Exception as e:
        print("Exception when calling CatalogAPI->table_exists: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **table** | **str**| A table name | 

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
**404** | Not Found - NoSuchTableException, Table not found |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_properties**
> UpdateNamespacePropertiesResponse update_properties(prefix, namespace, update_namespace_properties_request)

Set or remove properties on a namespace

Set and/or remove properties on a namespace. The request body specifies a list of properties to remove and a map of key value pairs to update.
Properties that are not in the request are not modified or removed by this call.
Server implementations are not required to support namespace properties.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.update_namespace_properties_request import UpdateNamespacePropertiesRequest
from polaris.catalog.models.update_namespace_properties_response import UpdateNamespacePropertiesResponse
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    update_namespace_properties_request = polaris.catalog.UpdateNamespacePropertiesRequest() # UpdateNamespacePropertiesRequest | 

    try:
        # Set or remove properties on a namespace
        api_response = api_instance.update_properties(prefix, namespace, update_namespace_properties_request)
        print("The response of CatalogAPI->update_properties:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->update_properties: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **update_namespace_properties_request** | [**UpdateNamespacePropertiesRequest**](UpdateNamespacePropertiesRequest.md)|  | 

### Return type

[**UpdateNamespacePropertiesResponse**](UpdateNamespacePropertiesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | JSON data response for a synchronous update properties request. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - Namespace not found |  -  |
**406** | Not Acceptable / Unsupported Operation. The server does not support this operation. |  -  |
**422** | Unprocessable Entity - A property key was included in both &#x60;removals&#x60; and &#x60;updates&#x60; |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_table**
> CommitTableResponse update_table(prefix, namespace, table, commit_table_request)

Commit updates to a table

Commit updates to a table.

Commits have two parts, requirements and updates. Requirements are assertions that will be validated before attempting to make and commit changes. For example, `assert-ref-snapshot-id` will check that a named ref's snapshot ID has a certain value. Server implementations are required to fail with a 400 status code if any unknown updates or requirements are received.

Updates are changes to make to table metadata. For example, after asserting that the current main ref is at the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id.

Create table transactions that are started by createTable with `stage-create` set to true are committed using this route. Transactions should include all changes to the table, including table initialization, like AddSchemaUpdate and SetCurrentSchemaUpdate. The `assert-create` requirement is used to ensure that the table was not created concurrently.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.commit_table_request import CommitTableRequest
from polaris.catalog.models.commit_table_response import CommitTableResponse
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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    table = 'sales' # str | A table name
    commit_table_request = polaris.catalog.CommitTableRequest() # CommitTableRequest | 

    try:
        # Commit updates to a table
        api_response = api_instance.update_table(prefix, namespace, table, commit_table_request)
        print("The response of CatalogAPI->update_table:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CatalogAPI->update_table: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **table** | **str**| A table name | 
 **commit_table_request** | [**CommitTableRequest**](CommitTableRequest.md)|  | 

### Return type

[**CommitTableResponse**](CommitTableResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Response used when a table is successfully updated. The table metadata JSON is returned in the metadata field. The corresponding file location of table metadata must be returned in the metadata-location field. Clients can check whether metadata has changed by comparing metadata locations. |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableException, table to load does not exist |  -  |
**409** | Conflict - CommitFailedException, one or more requirements failed. The client may retry. |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**500** | An unknown server-side problem occurred; the commit state is unknown. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**502** | A gateway or proxy received an invalid response from the upstream server; the commit state is unknown. |  -  |
**504** | A server-side gateway timeout occurred; the commit state is unknown. |  -  |
**5XX** | A server-side problem that might not be addressable on the client. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **view_exists**
> view_exists(prefix, namespace, view)

Check if a view exists

Check if a view exists within a given namespace. This request does not return a response body.

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
    api_instance = polaris.catalog.CatalogAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    view = 'sales' # str | A view name

    try:
        # Check if a view exists
        api_instance.view_exists(prefix, namespace, view)
    except Exception as e:
        print("Exception when calling CatalogAPI->view_exists: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **view** | **str**| A view name | 

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
**400** | Bad Request |  -  |
**401** | Unauthorized |  -  |
**404** | Not Found |  -  |
**419** | Credentials have timed out. If possible, the client should refresh credentials and retry. |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

