# PrincipalRoles


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**roles** | [**List[PrincipalRole]**](PrincipalRole.md) |  | 

## Example

```python
from polaris.management.models.principal_roles import PrincipalRoles

# TODO update the JSON string below
json = "{}"
# create an instance of PrincipalRoles from a JSON string
principal_roles_instance = PrincipalRoles.from_json(json)
# print the JSON string representation of the object
print(PrincipalRoles.to_json())

# convert the object into a dict
principal_roles_dict = principal_roles_instance.to_dict()
# create an instance of PrincipalRoles from a dict
principal_roles_from_dict = PrincipalRoles.from_dict(principal_roles_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


