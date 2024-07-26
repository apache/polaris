# CreateCatalogRoleRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**catalog_role** | [**CatalogRole**](CatalogRole.md) |  | [optional] 

## Example

```python
from polaris.management.models.create_catalog_role_request import CreateCatalogRoleRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateCatalogRoleRequest from a JSON string
create_catalog_role_request_instance = CreateCatalogRoleRequest.from_json(json)
# print the JSON string representation of the object
print(CreateCatalogRoleRequest.to_json())

# convert the object into a dict
create_catalog_role_request_dict = create_catalog_role_request_instance.to_dict()
# create an instance of CreateCatalogRoleRequest from a dict
create_catalog_role_request_from_dict = CreateCatalogRoleRequest.from_dict(create_catalog_role_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


