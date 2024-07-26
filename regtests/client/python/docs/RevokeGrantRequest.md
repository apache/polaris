# RevokeGrantRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**grant** | [**GrantResource**](GrantResource.md) |  | [optional] 

## Example

```python
from polaris.management.models.revoke_grant_request import RevokeGrantRequest

# TODO update the JSON string below
json = "{}"
# create an instance of RevokeGrantRequest from a JSON string
revoke_grant_request_instance = RevokeGrantRequest.from_json(json)
# print the JSON string representation of the object
print(RevokeGrantRequest.to_json())

# convert the object into a dict
revoke_grant_request_dict = revoke_grant_request_instance.to_dict()
# create an instance of RevokeGrantRequest from a dict
revoke_grant_request_from_dict = RevokeGrantRequest.from_dict(revoke_grant_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


