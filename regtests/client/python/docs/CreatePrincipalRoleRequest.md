# CreatePrincipalRoleRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**principal_role** | [**PrincipalRole**](PrincipalRole.md) |  | [optional] 

## Example

```python
from polaris.management.models.create_principal_role_request import CreatePrincipalRoleRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreatePrincipalRoleRequest from a JSON string
create_principal_role_request_instance = CreatePrincipalRoleRequest.from_json(json)
# print the JSON string representation of the object
print(CreatePrincipalRoleRequest.to_json())

# convert the object into a dict
create_principal_role_request_dict = create_principal_role_request_instance.to_dict()
# create an instance of CreatePrincipalRoleRequest from a dict
create_principal_role_request_from_dict = CreatePrincipalRoleRequest.from_dict(create_principal_role_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


