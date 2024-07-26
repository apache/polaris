# GrantCatalogRoleRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**catalog_role** | [**CatalogRole**](CatalogRole.md) |  | [optional] 

## Example

```python
from polaris.management.models.grant_catalog_role_request import GrantCatalogRoleRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GrantCatalogRoleRequest from a JSON string
grant_catalog_role_request_instance = GrantCatalogRoleRequest.from_json(json)
# print the JSON string representation of the object
print(GrantCatalogRoleRequest.to_json())

# convert the object into a dict
grant_catalog_role_request_dict = grant_catalog_role_request_instance.to_dict()
# create an instance of GrantCatalogRoleRequest from a dict
grant_catalog_role_request_from_dict = GrantCatalogRoleRequest.from_dict(grant_catalog_role_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


