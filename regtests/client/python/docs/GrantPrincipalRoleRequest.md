# GrantPrincipalRoleRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**principal_role** | [**PrincipalRole**](PrincipalRole.md) |  | [optional] 

## Example

```python
from polaris.management.models.grant_principal_role_request import GrantPrincipalRoleRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GrantPrincipalRoleRequest from a JSON string
grant_principal_role_request_instance = GrantPrincipalRoleRequest.from_json(json)
# print the JSON string representation of the object
print(GrantPrincipalRoleRequest.to_json())

# convert the object into a dict
grant_principal_role_request_dict = grant_principal_role_request_instance.to_dict()
# create an instance of GrantPrincipalRoleRequest from a dict
grant_principal_role_request_from_dict = GrantPrincipalRoleRequest.from_dict(grant_principal_role_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


