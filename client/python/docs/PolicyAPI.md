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
# polaris.catalog.PolicyAPI

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**attach_policy**](PolicyAPI.md#attach_policy) | **PUT** /polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}/mappings | Create a mapping between a policy and a resource entity
[**create_policy**](PolicyAPI.md#create_policy) | **POST** /polaris/v1/{prefix}/namespaces/{namespace}/policies | Create a policy in the given namespace
[**detach_policy**](PolicyAPI.md#detach_policy) | **POST** /polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}/mappings | Remove a mapping between a policy and a target entity
[**drop_policy**](PolicyAPI.md#drop_policy) | **DELETE** /polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name} | Drop a policy from the catalog
[**get_applicable_policies**](PolicyAPI.md#get_applicable_policies) | **GET** /polaris/v1/{prefix}/applicable-policies | Get Applicable policies for catalog, namespace, table, or views
[**list_policies**](PolicyAPI.md#list_policies) | **GET** /polaris/v1/{prefix}/namespaces/{namespace}/policies | List all policy identifiers underneath a given namespace
[**load_policy**](PolicyAPI.md#load_policy) | **GET** /polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name} | Load a policy
[**update_policy**](PolicyAPI.md#update_policy) | **PUT** /polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name} | Update a policy


# **attach_policy**
> attach_policy(prefix, namespace, policy_name, attach_policy_request)

Create a mapping between a policy and a resource entity

Create a mapping between a policy and a resource entity

Policy can be attached to different levels:
1. **Table-like level:** Policies specific to individual tables or views.
2. **Namespace level:** Policies applies to a namespace.
3. **Catalog level:** Policies that applies to a catalog

The ability to attach a policy to a specific entity type is governed by the PolicyValidator. A policy can only be attached if the resource entity is a valid target for the specified policy type.

In addition to the validation rules enforced by the PolicyValidator, there are additional constraints on policy attachment:
1. For inheritable policies, only one policy of the same type can be attached to a given resource entity.
2. For non-inheritable policies, multiple policies of the same type can be attached to the same resource entity without restriction.

For inheritable policies, the inheritance override rule is:
1. Table-like level policies override namespace and catalog policies.
2. Namespace-level policies override upper level namespace or catalog policies.

Additional parameters can be provided in `parameters` when creating a mapping to define specific behavior or constraints.

If the policy is already attached to the target entity, the existing mapping record will be updated with the new set of parameters, replacing the previous ones.


### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.attach_policy_request import AttachPolicyRequest
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
    api_instance = polaris.catalog.PolicyAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    policy_name = 'policy_name_example' # str | 
    attach_policy_request = polaris.catalog.AttachPolicyRequest() # AttachPolicyRequest | 

    try:
        # Create a mapping between a policy and a resource entity
        api_instance.attach_policy(prefix, namespace, policy_name, attach_policy_request)
    except Exception as e:
        print("Exception when calling PolicyAPI->attach_policy: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **policy_name** | **str**|  | 
 **attach_policy_request** | [**AttachPolicyRequest**](AttachPolicyRequest.md)|  | 

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
**404** | Not Found - NoSuchPolicyException, NoSuchTargetException |  -  |
**409** | Conflict - The policy type is inheritable and there is already a policy of the same type attached to the target entity |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_policy**
> LoadPolicyResponse create_policy(prefix, namespace, create_policy_request)

Create a policy in the given namespace

Creates a policy within the specified namespace.

A policy defines a set of rules governing actions on specified resources under predefined conditions.
In Apache Polaris, policies are created, stored, and later referenced by external engines to enforce access controls on associated resources.

User provides the following inputs when creating a policy
- `name` (REQUIRED): The name of the policy.
- `type` (REQUIRED): The type of the policy.
  - **Predefined Policies:** policies have a `system.*` prefix in their type, such as `system.data_compaction`
- `description` (OPTIONAL): Provides details about the policy's purpose and functionality
- `content` (OPTIONAL): Defines the rules that control actions and access conditions on resources. The format can be JSON, SQL, or any other format.

The content field in the request body is validated using the policy's corresponding validator. The policy is created only if the content passes validation.

Upon successful creation, the new policy's version will be 0.


### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.create_policy_request import CreatePolicyRequest
from polaris.catalog.models.load_policy_response import LoadPolicyResponse
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
    api_instance = polaris.catalog.PolicyAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    create_policy_request = polaris.catalog.CreatePolicyRequest() # CreatePolicyRequest | 

    try:
        # Create a policy in the given namespace
        api_response = api_instance.create_policy(prefix, namespace, create_policy_request)
        print("The response of PolicyAPI->create_policy:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolicyAPI->create_policy: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **create_policy_request** | [**CreatePolicyRequest**](CreatePolicyRequest.md)|  | 

### Return type

[**LoadPolicyResponse**](LoadPolicyResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Policy object result after creating a policy |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**409** | Conflict - The policy already exists under the given namespace |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **detach_policy**
> detach_policy(prefix, namespace, policy_name, detach_policy_request)

Remove a mapping between a policy and a target entity

Remove a mapping between a policy and a target entity

A target entity can be a catalog, namespace, table or view.


### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.detach_policy_request import DetachPolicyRequest
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
    api_instance = polaris.catalog.PolicyAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    policy_name = 'policy_name_example' # str | 
    detach_policy_request = polaris.catalog.DetachPolicyRequest() # DetachPolicyRequest | 

    try:
        # Remove a mapping between a policy and a target entity
        api_instance.detach_policy(prefix, namespace, policy_name, detach_policy_request)
    except Exception as e:
        print("Exception when calling PolicyAPI->detach_policy: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **policy_name** | **str**|  | 
 **detach_policy_request** | [**DetachPolicyRequest**](DetachPolicyRequest.md)|  | 

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
**404** | Not Found - NoSuchPolicyException, NoSuchTargetException, NoSuchMappingException |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **drop_policy**
> drop_policy(prefix, namespace, policy_name, detach_all=detach_all)

Drop a policy from the catalog

Remove a policy from the catalog. 

A policy can only be dropped if it is not attached to any resource entity. To remove the policy along with all its attachments, set detach-all to true.


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
    api_instance = polaris.catalog.PolicyAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    policy_name = 'policy_name_example' # str | 
    detach_all = false # bool | When set to true, the dropPolicy operation will also delete all mappings between the policy and its attached target entities.  (optional)

    try:
        # Drop a policy from the catalog
        api_instance.drop_policy(prefix, namespace, policy_name, detach_all=detach_all)
    except Exception as e:
        print("Exception when calling PolicyAPI->drop_policy: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **policy_name** | **str**|  | 
 **detach_all** | **bool**| When set to true, the dropPolicy operation will also delete all mappings between the policy and its attached target entities.  | [optional] 

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
**400** | Bad Request - the policy to be dropped is attached to one or more targets and detach-all is not set to true |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchPolicyException, policy to get does not exist |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_applicable_policies**
> GetApplicablePoliciesResponse get_applicable_policies(prefix, page_token=page_token, page_size=page_size, namespace=namespace, target_name=target_name, policy_type=policy_type)

Get Applicable policies for catalog, namespace, table, or views

Retrieves all applicable policies for a specified entity, including inherited policies from parent entities. An entity can be a table/view, namespace, or catalog. The required parameters depend on the entity type:

- Table/View:
  - The `namespace` parameter is required to specify the entity's namespace.
  - The `target-name` parameter is required to specify the entity name.
- Namespace:
  - The `namespace` parameter is required to specify the identifier.
  - The `target-name` parameter should not be set.
- Catalog:
  - Neither `namespace` nor `target-name` should be set.

An optional policyType parameter filters results to return only policies of the specified type.

This API evaluates the entity's hierarchy and applies inheritable policies from parent entities. The inheritance follows the following override rule:

1. Table-like level policies override namespace and catalog policies.
2. Namespace-level policies override upper level namespace or catalog policies.


### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.get_applicable_policies_response import GetApplicablePoliciesResponse
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
    api_instance = polaris.catalog.PolicyAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    page_token = 'page_token_example' # str |  (optional)
    page_size = 56 # int | For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`. (optional)
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte. (optional)
    target_name = 'test_table' # str | Name of the target table/view (optional)
    policy_type = 'policy_type_example' # str |  (optional)

    try:
        # Get Applicable policies for catalog, namespace, table, or views
        api_response = api_instance.get_applicable_policies(prefix, page_token=page_token, page_size=page_size, namespace=namespace, target_name=target_name, policy_type=policy_type)
        print("The response of PolicyAPI->get_applicable_policies:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolicyAPI->get_applicable_policies: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **page_token** | **str**|  | [optional] 
 **page_size** | **int**| For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;. | [optional] 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | [optional] 
 **target_name** | **str**| Name of the target table/view | [optional] 
 **policy_type** | **str**|  | [optional] 

### Return type

[**GetApplicablePoliciesResponse**](GetApplicablePoliciesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | A list of policies applicable to the table |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchTableException, target table does not exist - NoSuchViewException, target view does not exist - NoSuchNamespaceException, target namespace does not exist |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_policies**
> ListPoliciesResponse list_policies(prefix, namespace, page_token=page_token, page_size=page_size, policy_type=policy_type)

List all policy identifiers underneath a given namespace

Return all policy identifiers under this namespace. Users can optionally filter the result by policy type

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.list_policies_response import ListPoliciesResponse
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
    api_instance = polaris.catalog.PolicyAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    page_token = 'page_token_example' # str |  (optional)
    page_size = 56 # int | For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated `pageSize`. (optional)
    policy_type = 'policy_type_example' # str |  (optional)

    try:
        # List all policy identifiers underneath a given namespace
        api_response = api_instance.list_policies(prefix, namespace, page_token=page_token, page_size=page_size, policy_type=policy_type)
        print("The response of PolicyAPI->list_policies:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolicyAPI->list_policies: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **page_token** | **str**|  | [optional] 
 **page_size** | **int**| For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;. | [optional] 
 **policy_type** | **str**|  | [optional] 

### Return type

[**ListPoliciesResponse**](ListPoliciesResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | a list of policy identifiers |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - The namespace specified does not exist |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **load_policy**
> LoadPolicyResponse load_policy(prefix, namespace, policy_name)

Load a policy

Load a policy from the catalog

The response contains the policy's metadata and content. For more details, refer to the definition of the `Policy` model.


### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.load_policy_response import LoadPolicyResponse
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
    api_instance = polaris.catalog.PolicyAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    policy_name = 'policy_name_example' # str | 

    try:
        # Load a policy
        api_response = api_instance.load_policy(prefix, namespace, policy_name)
        print("The response of PolicyAPI->load_policy:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolicyAPI->load_policy: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **policy_name** | **str**|  | 

### Return type

[**LoadPolicyResponse**](LoadPolicyResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Policy object result when getting a policy |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchPolicyException, policy to get does not exist |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_policy**
> LoadPolicyResponse update_policy(prefix, namespace, policy_name, update_policy_request)

Update a policy

Update a policy

A policy's description and content can be updated. The new content is validated against the policy's corresponding validator.
Upon a successful update, the policy's version is incremented by 1.

The update will only succeed if the current version matches the one in the catalog.


### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.load_policy_response import LoadPolicyResponse
from polaris.catalog.models.update_policy_request import UpdatePolicyRequest
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
    api_instance = polaris.catalog.PolicyAPI(api_client)
    prefix = 'prefix_example' # str | An optional prefix in the path
    namespace = 'accounting' # str | A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (`0x1F`) byte.
    policy_name = 'policy_name_example' # str | 
    update_policy_request = polaris.catalog.UpdatePolicyRequest() # UpdatePolicyRequest | 

    try:
        # Update a policy
        api_response = api_instance.update_policy(prefix, namespace, policy_name, update_policy_request)
        print("The response of PolicyAPI->update_policy:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling PolicyAPI->update_policy: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **prefix** | **str**| An optional prefix in the path | 
 **namespace** | **str**| A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte. | 
 **policy_name** | **str**|  | 
 **update_policy_request** | [**UpdatePolicyRequest**](UpdatePolicyRequest.md)|  | 

### Return type

[**LoadPolicyResponse**](LoadPolicyResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Response used when a policy is successfully updated The updated policy JSON is returned in the policy field |  -  |
**400** | Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware. |  -  |
**401** | Unauthorized. Authentication is required and has failed or has not yet been provided. |  -  |
**403** | Forbidden. Authenticated user does not have the necessary permissions. |  -  |
**404** | Not Found - NoSuchPolicyException, policy to get does not exist |  -  |
**409** | The policy version doesn&#39;t match the current-policy-version; retry after fetching latest version |  -  |
**503** | The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry. |  -  |
**5XX** | A server-side problem that might not be addressable from the client side. Used for server 5xx errors without more specific documentation in individual routes. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

