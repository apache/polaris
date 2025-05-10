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
# ListPoliciesResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**next_page_token** | **str** | An opaque token that allows clients to make use of pagination for list APIs (e.g. ListTables). Clients may initiate the first paginated request by sending an empty query parameter &#x60;pageToken&#x60; to the server. Servers that support pagination should identify the &#x60;pageToken&#x60; parameter and return a &#x60;next-page-token&#x60; in the response if there are more results available.  After the initial request, the value of &#x60;next-page-token&#x60; from each response must be used as the &#x60;pageToken&#x60; parameter value for the next request. The server must return &#x60;null&#x60; value for the &#x60;next-page-token&#x60; in the last response. Servers that support pagination must return all results in a single response with the value of &#x60;next-page-token&#x60; set to &#x60;null&#x60; if the query parameter &#x60;pageToken&#x60; is not set in the request. Servers that do not support pagination should ignore the &#x60;pageToken&#x60; parameter and return all results in a single response. The &#x60;next-page-token&#x60; must be omitted from the response. Clients must interpret either &#x60;null&#x60; or missing response value of &#x60;next-page-token&#x60; as the end of the listing results. | [optional] 
**identifiers** | [**List[PolicyIdentifier]**](PolicyIdentifier.md) |  | [optional] 

## Example

```python
from polaris.catalog.models.list_policies_response import ListPoliciesResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ListPoliciesResponse from a JSON string
list_policies_response_instance = ListPoliciesResponse.from_json(json)
# print the JSON string representation of the object
print(ListPoliciesResponse.to_json())

# convert the object into a dict
list_policies_response_dict = list_policies_response_instance.to_dict()
# create an instance of ListPoliciesResponse from a dict
list_policies_response_from_dict = ListPoliciesResponse.from_dict(list_policies_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


