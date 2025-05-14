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
# polaris.catalog.IcebergOAuth2API

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_token**](IcebergOAuth2API.md#get_token) | **POST** /v1/oauth/tokens | Get a token using an OAuth2 flow (DEPRECATED for REMOVAL)


# **get_token**
> OAuthTokenResponse get_token(grant_type=grant_type, scope=scope, client_id=client_id, client_secret=client_secret, requested_token_type=requested_token_type, subject_token=subject_token, subject_token_type=subject_token_type, actor_token=actor_token, actor_token_type=actor_token_type)

Get a token using an OAuth2 flow (DEPRECATED for REMOVAL)

The `oauth/tokens` endpoint is **DEPRECATED for REMOVAL**. It is _not_ recommended to implement this endpoint, unless you are fully aware of the potential security implications.
All clients are encouraged to explicitly set the configuration property `oauth2-server-uri` to the correct OAuth endpoint.
Deprecated since Iceberg (Java) 1.6.0. The endpoint and related types will be removed from this spec in Iceberg (Java) 2.0.
See [Security improvements in the Iceberg REST specification](https://github.com/apache/iceberg/issues/10537)

Exchange credentials for a token using the OAuth2 client credentials flow or token exchange.

This endpoint is used for three purposes -
1. To exchange client credentials (client ID and secret) for an access token This uses the client credentials flow.
2. To exchange a client token and an identity token for a more specific access token This uses the token exchange flow.
3. To exchange an access token for one with the same claims and a refreshed expiration period This uses the token exchange flow.

For example, a catalog client may be configured with client credentials from the OAuth2 Authorization flow. This client would exchange its client ID and secret for an access token using the client credentials request with this endpoint (1). Subsequent requests would then use that access token.

Some clients may also handle sessions that have additional user context. These clients would use the token exchange flow to exchange a user token (the "subject" token) from the session for a more specific access token for that user, using the catalog's access token as the "actor" token (2). The user ID token is the "subject" token and can be any token type allowed by the OAuth2 token exchange flow, including a unsecured JWT token with a sub claim. This request should use the catalog's bearer token in the "Authorization" header.

Clients may also use the token exchange flow to refresh a token that is about to expire by sending a token exchange request (3). The request's "subject" token should be the expiring token. This request should use the subject token in the "Authorization" header.

### Example

* OAuth Authentication (OAuth2):
* Bearer Authentication (BearerAuth):

```python
import polaris.catalog
from polaris.catalog.models.o_auth_token_response import OAuthTokenResponse
from polaris.catalog.models.token_type import TokenType
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
    api_instance = polaris.catalog.IcebergOAuth2API(api_client)
    grant_type = 'grant_type_example' # str |  (optional)
    scope = 'scope_example' # str |  (optional)
    client_id = 'client_id_example' # str | Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header. (optional)
    client_secret = 'client_secret_example' # str | Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header. (optional)
    requested_token_type = polaris.catalog.TokenType() # TokenType |  (optional)
    subject_token = 'subject_token_example' # str | Subject token for token exchange request (optional)
    subject_token_type = polaris.catalog.TokenType() # TokenType |  (optional)
    actor_token = 'actor_token_example' # str | Actor token for token exchange request (optional)
    actor_token_type = polaris.catalog.TokenType() # TokenType |  (optional)

    try:
        # Get a token using an OAuth2 flow (DEPRECATED for REMOVAL)
        api_response = api_instance.get_token(grant_type=grant_type, scope=scope, client_id=client_id, client_secret=client_secret, requested_token_type=requested_token_type, subject_token=subject_token, subject_token_type=subject_token_type, actor_token=actor_token, actor_token_type=actor_token_type)
        print("The response of IcebergOAuth2API->get_token:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling IcebergOAuth2API->get_token: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **grant_type** | **str**|  | [optional] 
 **scope** | **str**|  | [optional] 
 **client_id** | **str**| Client ID  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header. | [optional] 
 **client_secret** | **str**| Client secret  This can be sent in the request body, but OAuth2 recommends sending it in a Basic Authorization header. | [optional] 
 **requested_token_type** | [**TokenType**](TokenType.md)|  | [optional] 
 **subject_token** | **str**| Subject token for token exchange request | [optional] 
 **subject_token_type** | [**TokenType**](TokenType.md)|  | [optional] 
 **actor_token** | **str**| Actor token for token exchange request | [optional] 
 **actor_token_type** | [**TokenType**](TokenType.md)|  | [optional] 

### Return type

[**OAuthTokenResponse**](OAuthTokenResponse.md)

### Authorization

[OAuth2](../README.md#OAuth2), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

 - **Content-Type**: application/x-www-form-urlencoded
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | OAuth2 token response for client credentials or token exchange |  -  |
**400** | OAuth2 error response |  -  |
**401** | OAuth2 error response |  -  |
**5XX** | OAuth2 error response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

