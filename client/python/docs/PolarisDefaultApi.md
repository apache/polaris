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
# polaris.management.PolarisDefaultApi

All URIs are relative to *https://localhost/api/management/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_grant_to_catalog_role**](PolarisDefaultApi.md#add_grant_to_catalog_role) | **PUT** /catalogs/{catalogName}/catalog-roles/{catalogRoleName}/grants | 
[**assign_catalog_role_to_principal_role**](PolarisDefaultApi.md#assign_catalog_role_to_principal_role) | **PUT** /principal-roles/{principalRoleName}/catalog-roles/{catalogName} | 
[**assign_principal_role**](PolarisDefaultApi.md#assign_principal_role) | **PUT** /principals/{principalName}/principal-roles | 
[**create_catalog**](PolarisDefaultApi.md#create_catalog) | **POST** /catalogs | 
[**create_catalog_role**](PolarisDefaultApi.md#create_catalog_role) | **POST** /catalogs/{catalogName}/catalog-roles | 
[**create_principal**](PolarisDefaultApi.md#create_principal) | **POST** /principals | 
[**create_principal_role**](PolarisDefaultApi.md#create_principal_role) | **POST** /principal-roles | 
[**delete_catalog**](PolarisDefaultApi.md#delete_catalog) | **DELETE** /catalogs/{catalogName} | 
[**delete_catalog_role**](PolarisDefaultApi.md#delete_catalog_role) | **DELETE** /catalogs/{catalogName}/catalog-roles/{catalogRoleName} | 
[**delete_principal**](PolarisDefaultApi.md#delete_principal) | **DELETE** /principals/{principalName} | 
[**delete_principal_role**](PolarisDefaultApi.md#delete_principal_role) | **DELETE** /principal-roles/{principalRoleName} | 
[**get_catalog**](PolarisDefaultApi.md#get_catalog) | **GET** /catalogs/{catalogName} | 
[**get_catalog_role**](PolarisDefaultApi.md#get_catalog_role) | **GET** /catalogs/{catalogName}/catalog-roles/{catalogRoleName} | 
[**get_principal**](PolarisDefaultApi.md#get_principal) | **GET** /principals/{principalName} | 
[**get_principal_role**](PolarisDefaultApi.md#get_principal_role) | **GET** /principal-roles/{principalRoleName} | 
[**list_assignee_principal_roles_for_catalog_role**](PolarisDefaultApi.md#list_assignee_principal_roles_for_catalog_role) | **GET** /catalogs/{catalogName}/catalog-roles/{catalogRoleName}/principal-roles | 
[**list_assignee_principals_for_principal_role**](PolarisDefaultApi.md#list_assignee_principals_for_principal_role) | **GET** /principal-roles/{principalRoleName}/principals | 
[**list_catalog_roles**](PolarisDefaultApi.md#list_catalog_roles) | **GET** /catalogs/{catalogName}/catalog-roles | 
[**list_catalog_roles_for_principal_role**](PolarisDefaultApi.md#list_catalog_roles_for_principal_role) | **GET** /principal-roles/{principalRoleName}/catalog-roles/{catalogName} | 
[**list_catalogs**](PolarisDefaultApi.md#list_catalogs) | **GET** /catalogs | 
[**list_grants_for_catalog_role**](PolarisDefaultApi.md#list_grants_for_catalog_role) | **GET** /catalogs/{catalogName}/catalog-roles/{catalogRoleName}/grants | 
[**list_principal_roles**](PolarisDefaultApi.md#list_principal_roles) | **GET** /principal-roles | 
[**list_principal_roles_assigned**](PolarisDefaultApi.md#list_principal_roles_assigned) | **GET** /principals/{principalName}/principal-roles | 
[**list_principals**](PolarisDefaultApi.md#list_principals) | **GET** /principals | 
[**revoke_catalog_role_from_principal_role**](PolarisDefaultApi.md#revoke_catalog_role_from_principal_role) | **DELETE** /principal-roles/{principalRoleName}/catalog-roles/{catalogName}/{catalogRoleName} | 
[**revoke_grant_from_catalog_role**](PolarisDefaultApi.md#revoke_grant_from_catalog_role) | **POST** /catalogs/{catalogName}/catalog-roles/{catalogRoleName}/grants | 
[**revoke_principal_role**](PolarisDefaultApi.md#revoke_principal_role) | **DELETE** /principals/{principalName}/principal-roles/{principalRoleName} | 
[**rotate_credentials**](PolarisDefaultApi.md#rotate_credentials) | **POST** /principals/{principalName}/rotate | 
[**update_catalog**](PolarisDefaultApi.md#update_catalog) | **PUT** /catalogs/{catalogName} | 
[**update_catalog_role**](PolarisDefaultApi.md#update_catalog_role) | **PUT** /catalogs/{catalogName}/catalog-roles/{catalogRoleName} | 
[**update_principal**](PolarisDefaultApi.md#update_principal) | **PUT** /principals/{principalName} | 
[**update_principal_role**](PolarisDefaultApi.md#update_principal_role) | **PUT** /principal-roles/{principalRoleName} | 


# **add_grant_to_catalog_role**
> add_grant_to_catalog_role(catalog_name, catalog_role_name, add_grant_request=add_grant_request)

Add a new grant to the catalog role

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.add_grant_request import AddGrantRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The name of the catalog where the role will receive the grant
    catalog_role_name = 'catalog_role_name_example' # str | The name of the role receiving the grant (must exist)
    add_grant_request = polaris.management.AddGrantRequest() # AddGrantRequest |  (optional)

    try:
        api_instance.add_grant_to_catalog_role(catalog_name, catalog_role_name, add_grant_request=add_grant_request)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->add_grant_to_catalog_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The name of the catalog where the role will receive the grant | 
 **catalog_role_name** | **str**| The name of the role receiving the grant (must exist) | 
 **add_grant_request** | [**AddGrantRequest**](AddGrantRequest.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful response |  -  |
**403** | The principal is not authorized to create grants |  -  |
**404** | The catalog or the role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **assign_catalog_role_to_principal_role**
> assign_catalog_role_to_principal_role(principal_role_name, catalog_name, grant_catalog_role_request)

Assign a catalog role to a principal role

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.grant_catalog_role_request import GrantCatalogRoleRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_role_name = 'principal_role_name_example' # str | The principal role name
    catalog_name = 'catalog_name_example' # str | The name of the catalog where the catalogRoles reside
    grant_catalog_role_request = polaris.management.GrantCatalogRoleRequest() # GrantCatalogRoleRequest | The principal to create

    try:
        api_instance.assign_catalog_role_to_principal_role(principal_role_name, catalog_name, grant_catalog_role_request)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->assign_catalog_role_to_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_role_name** | **str**| The principal role name | 
 **catalog_name** | **str**| The name of the catalog where the catalogRoles reside | 
 **grant_catalog_role_request** | [**GrantCatalogRoleRequest**](GrantCatalogRoleRequest.md)| The principal to create | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful response |  -  |
**403** | The caller does not have permission to assign a catalog role |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **assign_principal_role**
> assign_principal_role(principal_name, grant_principal_role_request)

Add a role to the principal

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.grant_principal_role_request import GrantPrincipalRoleRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_name = 'principal_name_example' # str | The name of the target principal
    grant_principal_role_request = polaris.management.GrantPrincipalRoleRequest() # GrantPrincipalRoleRequest | The principal role to assign

    try:
        api_instance.assign_principal_role(principal_name, grant_principal_role_request)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->assign_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_name** | **str**| The name of the target principal | 
 **grant_principal_role_request** | [**GrantPrincipalRoleRequest**](GrantPrincipalRoleRequest.md)| The principal role to assign | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful response |  -  |
**403** | The caller does not have permission to add assign a role to the principal |  -  |
**404** | The catalog, the principal, or the role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_catalog**
> create_catalog(create_catalog_request)

Add a new Catalog

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.create_catalog_request import CreateCatalogRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    create_catalog_request = polaris.management.CreateCatalogRequest() # CreateCatalogRequest | The Catalog to create

    try:
        api_instance.create_catalog(create_catalog_request)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->create_catalog: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_catalog_request** | [**CreateCatalogRequest**](CreateCatalogRequest.md)| The Catalog to create | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful response |  -  |
**403** | The caller does not have permission to create a catalog |  -  |
**404** | The catalog does not exist |  -  |
**409** | A catalog with the specified name already exists |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_catalog_role**
> create_catalog_role(catalog_name, create_catalog_role_request=create_catalog_role_request)

Create a new role in the catalog

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.create_catalog_role_request import CreateCatalogRoleRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The catalog for which we are reading/updating roles
    create_catalog_role_request = polaris.management.CreateCatalogRoleRequest() # CreateCatalogRoleRequest |  (optional)

    try:
        api_instance.create_catalog_role(catalog_name, create_catalog_role_request=create_catalog_role_request)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->create_catalog_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The catalog for which we are reading/updating roles | 
 **create_catalog_role_request** | [**CreateCatalogRoleRequest**](CreateCatalogRoleRequest.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful response |  -  |
**403** | The principal is not authorized to create roles |  -  |
**404** | The catalog does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_principal**
> PrincipalWithCredentials create_principal(create_principal_request)

Create a principal

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.create_principal_request import CreatePrincipalRequest
from polaris.management.models.principal_with_credentials import PrincipalWithCredentials
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    create_principal_request = polaris.management.CreatePrincipalRequest() # CreatePrincipalRequest | The principal to create

    try:
        api_response = api_instance.create_principal(create_principal_request)
        print("The response of PolarisDefaultApi->create_principal:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->create_principal: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_principal_request** | [**CreatePrincipalRequest**](CreatePrincipalRequest.md)| The principal to create | 

### Return type

[**PrincipalWithCredentials**](PrincipalWithCredentials.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful response |  -  |
**403** | The caller does not have permission to add a principal |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_principal_role**
> create_principal_role(create_principal_role_request)

Create a principal role

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.create_principal_role_request import CreatePrincipalRoleRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    create_principal_role_request = polaris.management.CreatePrincipalRoleRequest() # CreatePrincipalRoleRequest | The principal to create

    try:
        api_instance.create_principal_role(create_principal_role_request)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->create_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_principal_role_request** | [**CreatePrincipalRoleRequest**](CreatePrincipalRoleRequest.md)| The principal to create | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful response |  -  |
**403** | The caller does not have permission to add a principal role |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_catalog**
> delete_catalog(catalog_name)

Delete an existing catalog. The catalog must be empty.

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The name of the catalog

    try:
        api_instance.delete_catalog(catalog_name)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->delete_catalog: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The name of the catalog | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**403** | The caller does not have permission to delete a catalog |  -  |
**404** | The catalog does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_catalog_role**
> delete_catalog_role(catalog_name, catalog_role_name)

Delete an existing role from the catalog. All associated grants will also be deleted

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The catalog for which we are retrieving roles
    catalog_role_name = 'catalog_role_name_example' # str | The name of the role

    try:
        api_instance.delete_catalog_role(catalog_name, catalog_role_name)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->delete_catalog_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The catalog for which we are retrieving roles | 
 **catalog_role_name** | **str**| The name of the role | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**403** | The principal is not authorized to delete roles |  -  |
**404** | The catalog or the role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_principal**
> delete_principal(principal_name)

Remove a principal from polaris

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_name = 'principal_name_example' # str | The principal name

    try:
        api_instance.delete_principal(principal_name)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->delete_principal: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_name** | **str**| The principal name | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**403** | The caller does not have permission to delete a principal |  -  |
**404** | The principal does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_principal_role**
> delete_principal_role(principal_role_name)

Remove a principal role from polaris

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_role_name = 'principal_role_name_example' # str | The principal role name

    try:
        api_instance.delete_principal_role(principal_role_name)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->delete_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_role_name** | **str**| The principal role name | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**403** | The caller does not have permission to delete a principal role |  -  |
**404** | The principal role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_catalog**
> Catalog get_catalog(catalog_name)

Get the details of a catalog

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.catalog import Catalog
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The name of the catalog

    try:
        api_response = api_instance.get_catalog(catalog_name)
        print("The response of PolarisDefaultApi->get_catalog:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->get_catalog: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The name of the catalog | 

### Return type

[**Catalog**](Catalog.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The catalog details |  -  |
**403** | The caller does not have permission to read catalog details |  -  |
**404** | The catalog does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_catalog_role**
> CatalogRole get_catalog_role(catalog_name, catalog_role_name)

Get the details of an existing role

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.catalog_role import CatalogRole
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The catalog for which we are retrieving roles
    catalog_role_name = 'catalog_role_name_example' # str | The name of the role

    try:
        api_response = api_instance.get_catalog_role(catalog_name, catalog_role_name)
        print("The response of PolarisDefaultApi->get_catalog_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->get_catalog_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The catalog for which we are retrieving roles | 
 **catalog_role_name** | **str**| The name of the role | 

### Return type

[**CatalogRole**](CatalogRole.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The specified role details |  -  |
**403** | The principal is not authorized to read role data |  -  |
**404** | The catalog or the role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_principal**
> Principal get_principal(principal_name)

Get the principal details

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principal import Principal
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_name = 'principal_name_example' # str | The principal name

    try:
        api_response = api_instance.get_principal(principal_name)
        print("The response of PolarisDefaultApi->get_principal:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->get_principal: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_name** | **str**| The principal name | 

### Return type

[**Principal**](Principal.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The requested principal |  -  |
**403** | The caller does not have permission to get principal details |  -  |
**404** | The catalog or principal does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_principal_role**
> PrincipalRole get_principal_role(principal_role_name)

Get the principal role details

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principal_role import PrincipalRole
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_role_name = 'principal_role_name_example' # str | The principal role name

    try:
        api_response = api_instance.get_principal_role(principal_role_name)
        print("The response of PolarisDefaultApi->get_principal_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->get_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_role_name** | **str**| The principal role name | 

### Return type

[**PrincipalRole**](PrincipalRole.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The requested principal role |  -  |
**403** | The caller does not have permission to get principal role details |  -  |
**404** | The principal role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_assignee_principal_roles_for_catalog_role**
> PrincipalRoles list_assignee_principal_roles_for_catalog_role(catalog_name, catalog_role_name)

List the PrincipalRoles to which the target catalog role has been assigned

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principal_roles import PrincipalRoles
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The name of the catalog where the catalog role resides
    catalog_role_name = 'catalog_role_name_example' # str | The name of the catalog role

    try:
        api_response = api_instance.list_assignee_principal_roles_for_catalog_role(catalog_name, catalog_role_name)
        print("The response of PolarisDefaultApi->list_assignee_principal_roles_for_catalog_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_assignee_principal_roles_for_catalog_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The name of the catalog where the catalog role resides | 
 **catalog_role_name** | **str**| The name of the catalog role | 

### Return type

[**PrincipalRoles**](PrincipalRoles.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List the PrincipalRoles to which the target catalog role has been assigned |  -  |
**403** | The caller does not have permission to list principal roles |  -  |
**404** | The catalog or catalog role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_assignee_principals_for_principal_role**
> Principals list_assignee_principals_for_principal_role(principal_role_name)

List the Principals to whom the target principal role has been assigned

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principals import Principals
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_role_name = 'principal_role_name_example' # str | The principal role name

    try:
        api_response = api_instance.list_assignee_principals_for_principal_role(principal_role_name)
        print("The response of PolarisDefaultApi->list_assignee_principals_for_principal_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_assignee_principals_for_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_role_name** | **str**| The principal role name | 

### Return type

[**Principals**](Principals.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List the Principals to whom the target principal role has been assigned |  -  |
**403** | The caller does not have permission to list principals |  -  |
**404** | The principal role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_catalog_roles**
> CatalogRoles list_catalog_roles(catalog_name)

List existing roles in the catalog

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.catalog_roles import CatalogRoles
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The catalog for which we are reading/updating roles

    try:
        api_response = api_instance.list_catalog_roles(catalog_name)
        print("The response of PolarisDefaultApi->list_catalog_roles:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_catalog_roles: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The catalog for which we are reading/updating roles | 

### Return type

[**CatalogRoles**](CatalogRoles.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The list of roles that exist in this catalog |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_catalog_roles_for_principal_role**
> CatalogRoles list_catalog_roles_for_principal_role(principal_role_name, catalog_name)

Get the catalog roles mapped to the principal role

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.catalog_roles import CatalogRoles
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_role_name = 'principal_role_name_example' # str | The principal role name
    catalog_name = 'catalog_name_example' # str | The name of the catalog where the catalogRoles reside

    try:
        api_response = api_instance.list_catalog_roles_for_principal_role(principal_role_name, catalog_name)
        print("The response of PolarisDefaultApi->list_catalog_roles_for_principal_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_catalog_roles_for_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_role_name** | **str**| The principal role name | 
 **catalog_name** | **str**| The name of the catalog where the catalogRoles reside | 

### Return type

[**CatalogRoles**](CatalogRoles.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The list of catalog roles mapped to the principal role |  -  |
**403** | The caller does not have permission to list catalog roles |  -  |
**404** | The principal role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_catalogs**
> Catalogs list_catalogs()

List all catalogs in this polaris service

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.catalogs import Catalogs
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)

    try:
        api_response = api_instance.list_catalogs()
        print("The response of PolarisDefaultApi->list_catalogs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_catalogs: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**Catalogs**](Catalogs.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of catalogs in the polaris service |  -  |
**403** | The caller does not have permission to list catalog details |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_grants_for_catalog_role**
> GrantResources list_grants_for_catalog_role(catalog_name, catalog_role_name)

List the grants the catalog role holds

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.grant_resources import GrantResources
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The name of the catalog where the role will receive the grant
    catalog_role_name = 'catalog_role_name_example' # str | The name of the role receiving the grant (must exist)

    try:
        api_response = api_instance.list_grants_for_catalog_role(catalog_name, catalog_role_name)
        print("The response of PolarisDefaultApi->list_grants_for_catalog_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_grants_for_catalog_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The name of the catalog where the role will receive the grant | 
 **catalog_role_name** | **str**| The name of the role receiving the grant (must exist) | 

### Return type

[**GrantResources**](GrantResources.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of all grants given to the role in this catalog |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_principal_roles**
> PrincipalRoles list_principal_roles()

List the principal roles

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principal_roles import PrincipalRoles
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)

    try:
        api_response = api_instance.list_principal_roles()
        print("The response of PolarisDefaultApi->list_principal_roles:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_principal_roles: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**PrincipalRoles**](PrincipalRoles.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of principal roles |  -  |
**403** | The caller does not have permission to list principal roles |  -  |
**404** | The catalog does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_principal_roles_assigned**
> PrincipalRoles list_principal_roles_assigned(principal_name)

List the roles assigned to the principal

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principal_roles import PrincipalRoles
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_name = 'principal_name_example' # str | The name of the target principal

    try:
        api_response = api_instance.list_principal_roles_assigned(principal_name)
        print("The response of PolarisDefaultApi->list_principal_roles_assigned:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_principal_roles_assigned: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_name** | **str**| The name of the target principal | 

### Return type

[**PrincipalRoles**](PrincipalRoles.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of roles assigned to this principal |  -  |
**403** | The caller does not have permission to list roles |  -  |
**404** | The principal or catalog does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_principals**
> Principals list_principals()

List the principals for the current catalog

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principals import Principals
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)

    try:
        api_response = api_instance.list_principals()
        print("The response of PolarisDefaultApi->list_principals:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->list_principals: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**Principals**](Principals.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of principals for this catalog |  -  |
**403** | The caller does not have permission to list catalog admins |  -  |
**404** | The catalog does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **revoke_catalog_role_from_principal_role**
> revoke_catalog_role_from_principal_role(principal_role_name, catalog_name, catalog_role_name)

Remove a catalog role from a principal role

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_role_name = 'principal_role_name_example' # str | The principal role name
    catalog_name = 'catalog_name_example' # str | The name of the catalog that contains the role to revoke
    catalog_role_name = 'catalog_role_name_example' # str | The name of the catalog role that should be revoked

    try:
        api_instance.revoke_catalog_role_from_principal_role(principal_role_name, catalog_name, catalog_role_name)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->revoke_catalog_role_from_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_role_name** | **str**| The principal role name | 
 **catalog_name** | **str**| The name of the catalog that contains the role to revoke | 
 **catalog_role_name** | **str**| The name of the catalog role that should be revoked | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**403** | The caller does not have permission to revoke a catalog role |  -  |
**404** | The principal role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **revoke_grant_from_catalog_role**
> revoke_grant_from_catalog_role(catalog_name, catalog_role_name, cascade=cascade, revoke_grant_request=revoke_grant_request)

Delete a specific grant from the role. This may be a subset or a superset of the grants the role has. In case of a subset, the role will retain the grants not specified. If the `cascade` parameter is true, grant revocation will have a cascading effect - that is, if a principal has specific grants on a subresource, and grants are revoked on a parent resource, the grants present on the subresource will be revoked as well. By default, this behavior is disabled and grant revocation only affects the specified resource.

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.revoke_grant_request import RevokeGrantRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The name of the catalog where the role will receive the grant
    catalog_role_name = 'catalog_role_name_example' # str | The name of the role receiving the grant (must exist)
    cascade = False # bool | If true, the grant revocation cascades to all subresources. (optional) (default to False)
    revoke_grant_request = polaris.management.RevokeGrantRequest() # RevokeGrantRequest |  (optional)

    try:
        api_instance.revoke_grant_from_catalog_role(catalog_name, catalog_role_name, cascade=cascade, revoke_grant_request=revoke_grant_request)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->revoke_grant_from_catalog_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The name of the catalog where the role will receive the grant | 
 **catalog_role_name** | **str**| The name of the role receiving the grant (must exist) | 
 **cascade** | **bool**| If true, the grant revocation cascades to all subresources. | [optional] [default to False]
 **revoke_grant_request** | [**RevokeGrantRequest**](RevokeGrantRequest.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful response |  -  |
**403** | The principal is not authorized to create grants |  -  |
**404** | The catalog or the role does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **revoke_principal_role**
> revoke_principal_role(principal_name, principal_role_name)

Remove a role from a catalog principal

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_name = 'principal_name_example' # str | The name of the target principal
    principal_role_name = 'principal_role_name_example' # str | The name of the role

    try:
        api_instance.revoke_principal_role(principal_name, principal_role_name)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->revoke_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_name** | **str**| The name of the target principal | 
 **principal_role_name** | **str**| The name of the role | 

### Return type

void (empty response body)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Success, no content |  -  |
**403** | The caller does not have permission to remove a role from the principal |  -  |
**404** | The catalog or principal does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **rotate_credentials**
> PrincipalWithCredentials rotate_credentials(principal_name)

Rotate a principal's credentials. The new credentials will be returned in the response. This is the only API, aside from createPrincipal, that returns the user's credentials. This API is *not* idempotent.

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principal_with_credentials import PrincipalWithCredentials
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_name = 'principal_name_example' # str | The user name

    try:
        api_response = api_instance.rotate_credentials(principal_name)
        print("The response of PolarisDefaultApi->rotate_credentials:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->rotate_credentials: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_name** | **str**| The user name | 

### Return type

[**PrincipalWithCredentials**](PrincipalWithCredentials.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The principal details along with the newly rotated credentials |  -  |
**403** | The caller does not have permission to rotate credentials |  -  |
**404** | The principal does not exist |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_catalog**
> Catalog update_catalog(catalog_name, update_catalog_request)

Update an existing catalog

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.catalog import Catalog
from polaris.management.models.update_catalog_request import UpdateCatalogRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The name of the catalog
    update_catalog_request = polaris.management.UpdateCatalogRequest() # UpdateCatalogRequest | The catalog details to use in the update

    try:
        api_response = api_instance.update_catalog(catalog_name, update_catalog_request)
        print("The response of PolarisDefaultApi->update_catalog:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->update_catalog: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The name of the catalog | 
 **update_catalog_request** | [**UpdateCatalogRequest**](UpdateCatalogRequest.md)| The catalog details to use in the update | 

### Return type

[**Catalog**](Catalog.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The catalog details |  -  |
**403** | The caller does not have permission to update catalog details |  -  |
**404** | The catalog does not exist |  -  |
**409** | The entity version doesn&#39;t match the currentEntityVersion; retry after fetching latest version |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_catalog_role**
> CatalogRole update_catalog_role(catalog_name, catalog_role_name, update_catalog_role_request=update_catalog_role_request)

Update an existing role in the catalog

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.catalog_role import CatalogRole
from polaris.management.models.update_catalog_role_request import UpdateCatalogRoleRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    catalog_name = 'catalog_name_example' # str | The catalog for which we are retrieving roles
    catalog_role_name = 'catalog_role_name_example' # str | The name of the role
    update_catalog_role_request = polaris.management.UpdateCatalogRoleRequest() # UpdateCatalogRoleRequest |  (optional)

    try:
        api_response = api_instance.update_catalog_role(catalog_name, catalog_role_name, update_catalog_role_request=update_catalog_role_request)
        print("The response of PolarisDefaultApi->update_catalog_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->update_catalog_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **catalog_name** | **str**| The catalog for which we are retrieving roles | 
 **catalog_role_name** | **str**| The name of the role | 
 **update_catalog_role_request** | [**UpdateCatalogRoleRequest**](UpdateCatalogRoleRequest.md)|  | [optional] 

### Return type

[**CatalogRole**](CatalogRole.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The specified role details |  -  |
**403** | The principal is not authorized to update roles |  -  |
**404** | The catalog or the role does not exist |  -  |
**409** | The entity version doesn&#39;t match the currentEntityVersion; retry after fetching latest version |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_principal**
> Principal update_principal(principal_name, update_principal_request)

Update an existing principal

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principal import Principal
from polaris.management.models.update_principal_request import UpdatePrincipalRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_name = 'principal_name_example' # str | The principal name
    update_principal_request = polaris.management.UpdatePrincipalRequest() # UpdatePrincipalRequest | The principal details to use in the update

    try:
        api_response = api_instance.update_principal(principal_name, update_principal_request)
        print("The response of PolarisDefaultApi->update_principal:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->update_principal: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_name** | **str**| The principal name | 
 **update_principal_request** | [**UpdatePrincipalRequest**](UpdatePrincipalRequest.md)| The principal details to use in the update | 

### Return type

[**Principal**](Principal.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The updated principal |  -  |
**403** | The caller does not have permission to update principal details |  -  |
**404** | The principal does not exist |  -  |
**409** | The entity version doesn&#39;t match the currentEntityVersion; retry after fetching latest version |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_principal_role**
> PrincipalRole update_principal_role(principal_role_name, update_principal_role_request)

Update an existing principalRole

### Example

* OAuth Authentication (OAuth2):

```python
import polaris.management
from polaris.management.models.principal_role import PrincipalRole
from polaris.management.models.update_principal_role_request import UpdatePrincipalRoleRequest
from polaris.management.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://localhost/api/management/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = polaris.management.Configuration(
    host = "https://localhost/api/management/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = os.environ["ACCESS_TOKEN"]

# Enter a context with an instance of the API client
with polaris.management.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = polaris.management.PolarisDefaultApi(api_client)
    principal_role_name = 'principal_role_name_example' # str | The principal role name
    update_principal_role_request = polaris.management.UpdatePrincipalRoleRequest() # UpdatePrincipalRoleRequest | The principalRole details to use in the update

    try:
        api_response = api_instance.update_principal_role(principal_role_name, update_principal_role_request)
        print("The response of PolarisDefaultApi->update_principal_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolarisDefaultApi->update_principal_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **principal_role_name** | **str**| The principal role name | 
 **update_principal_role_request** | [**UpdatePrincipalRoleRequest**](UpdatePrincipalRoleRequest.md)| The principalRole details to use in the update | 

### Return type

[**PrincipalRole**](PrincipalRole.md)

### Authorization

[OAuth2](../README.md#OAuth2)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The updated principal role |  -  |
**403** | The caller does not have permission to update principal role details |  -  |
**404** | The principal role does not exist |  -  |
**409** | The entity version doesn&#39;t match the currentEntityVersion; retry after fetching latest version |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

